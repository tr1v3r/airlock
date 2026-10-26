package airlock_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func priorityConfig(capacity int) airlock.Config {
	return airlock.Config{
		Capacity: capacity, MaxQueued: 96, MaxQueuedPerTenant: 32,
		Priority: &airlock.PriorityPolicy{
			Groups: []airlock.PriorityGroup{
				{Name: "high", Turns: 8, MaxQueued: 32},
				{Name: "medium", Turns: 3, MaxQueued: 32},
				{Name: "low", Turns: 1, MaxQueued: 32},
			},
			DefaultGroup: "low",
			TenantGroups: map[string]string{"A": "high", "B": "high", "M": "medium", "L": "low"},
		},
	}
}

func newPriority(t *testing.T, cfg airlock.Config) *airlock.Airlock {
	t.Helper()
	a, err := airlock.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func priorityEnqueue(ctx context.Context, t *testing.T, a *airlock.Airlock, ch chan<- result, id, tenant string, weight, count int) {
	t.Helper()
	go func() {
		var p *airlock.Permit
		var err error
		if tenant == "" {
			p, err = a.AcquireN(ctx, weight)
		} else {
			p, err = a.AcquireNFor(ctx, tenant, weight)
		}

		ch <- result{id, p, err}
	}()
	queued(t, a, count)
}

func priorityNext(t *testing.T, held *airlock.Permit, ch <-chan result, want string) *airlock.Permit {
	t.Helper()
	held.Release()
	r := receive(t, ch)
	if r.err != nil || r.id != want || r.p == nil {
		t.Fatalf("grant=%+v, want %s", r, want)
	}

	t.Cleanup(r.p.Release)
	return r.p
}

func TestPriorityConfigurationValidation(t *testing.T) {
	maxInt := int(^uint(0) >> 1)

	for _, tc := range []struct {
		name string
		edit func(*airlock.Config)
	}{
		{"no groups", func(c *airlock.Config) { c.Priority.Groups = nil }},
		{"too many groups", func(c *airlock.Config) { c.Priority.Groups = make([]airlock.PriorityGroup, 1025) }},
		{"empty group name", func(c *airlock.Config) { c.Priority.Groups[0].Name = "" }},
		{"duplicate group", func(c *airlock.Config) { c.Priority.Groups[1].Name = "high" }},
		{"zero turns", func(c *airlock.Config) { c.Priority.Groups[2].Turns = 0 }},
		{"negative turns", func(c *airlock.Config) { c.Priority.Groups[2].Turns = -1 }},
		{"increasing turns", func(c *airlock.Config) { c.Priority.Groups[1].Turns = 9 }},
		{"too many turns", func(c *airlock.Config) { c.Priority.Groups[0].Turns = 1024 }},
		{"turn sum overflow", func(c *airlock.Config) {
			c.Priority.Groups[0].Turns = maxInt
			c.Priority.Groups[1].Turns = maxInt
		}},
		{"negative group queue", func(c *airlock.Config) { c.Priority.Groups[0].MaxQueued = -1 }},
		{"excess group queues", func(c *airlock.Config) { c.Priority.Groups[0].MaxQueued++ }},
		{"group queue sum overflow", func(c *airlock.Config) {
			c.MaxQueued = maxInt
			c.Priority.Groups[0].MaxQueued = maxInt
			c.Priority.Groups[1].MaxQueued = maxInt
		}},
		{"empty default", func(c *airlock.Config) { c.Priority.DefaultGroup = "" }},
		{"unknown default", func(c *airlock.Config) { c.Priority.DefaultGroup = "missing" }},
		{"empty tenant", func(c *airlock.Config) { c.Priority.TenantGroups[""] = "high" }},
		{"unknown mapped group", func(c *airlock.Config) { c.Priority.TenantGroups["A"] = "missing" }},
		{"empty mapped group", func(c *airlock.Config) { c.Priority.TenantGroups["A"] = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := priorityConfig(1)
			tc.edit(&cfg)
			if a, err := airlock.New(cfg); a != nil || !errors.Is(err, airlock.ErrInvalidConfig) {
				t.Fatalf("New=%v,%v; want nil,ErrInvalidConfig", a, err)
			}
		})
	}

	for _, turns := range []int{1, 1024} {
		cfg := airlock.Config{Capacity: 1, Priority: &airlock.PriorityPolicy{
			Groups: []airlock.PriorityGroup{{Name: "only", Turns: turns}}, DefaultGroup: "only",
		}}
		a := newPriority(t, cfg)
		acquire(t, a, "unmapped").Release()
		assertWeightedIdle(t, a)
	}

	// Equal adjacent shares and the largest representable queue budget are valid.
	cfg := priorityConfig(1)
	cfg.MaxQueued = maxInt
	cfg.Priority.Groups[0].MaxQueued = maxInt - 64
	cfg.Priority.Groups[0].Turns = 3
	newPriority(t, cfg)
}

func TestPriorityCopiesConfigurationAndDefaultTenantIdentities(t *testing.T) {
	cfg := priorityConfig(1)
	cfg.MaxQueuedPerTenant = 1
	a := newPriority(t, cfg)
	// All three reference-containing layers belong to the caller, not Airlock.
	cfg.Priority.TenantGroups["A"] = "low"
	cfg.Priority.TenantGroups["unknown"] = "high"

	for i := range cfg.Priority.Groups {
		cfg.Priority.Groups[i].MaxQueued = 0
		cfg.Priority.Groups[i].Name = "changed"
		cfg.Priority.Groups[i].Turns = 0
	}

	cfg.Priority.DefaultGroup = "high"
	held := acquire(t, a, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 5)

	for i, tenant := range []string{"", "unknown", "default", "A", "B"} {
		priorityEnqueue(ctx, t, a, ch, fmt.Sprint(i), tenant, 1, i+1)
	}

	if s := a.Stats(); s.WaitingTenants != 5 {
		t.Fatalf("default and unmapped tenants must retain separate FIFO identities: %+v", s)
	}

	for _, want := range []string{"3", "4", "0", "1", "2"} {
		held = priorityNext(t, held, ch, want)
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestPrioritySaturatedSharesAndTenantFIFO(t *testing.T) {
	a := newPriority(t, priorityConfig(1))
	held := acquire(t, a, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 48)
	count := 0

	for _, tenant := range []string{"A", "B", "M", "L"} {
		for j := 0; j < 12; j++ {
			count++
			priorityEnqueue(ctx, t, a, ch, fmt.Sprintf("%s%d", tenant, j), tenant, 1, count)
		}
	}

	// Every group stays nonempty across both complete cycles. Each low grant
	// is separated by exactly eleven other selections; A/B alternate within H.
	for cycle := 0; cycle < 2; cycle++ {
		for j := 0; j < 8; j++ {
			tenant := []string{"A", "B"}[j%2]
			held = priorityNext(t, held, ch, fmt.Sprintf("%s%d", tenant, cycle*4+j/2))
		}

		for j := 0; j < 3; j++ {
			held = priorityNext(t, held, ch, fmt.Sprintf("M%d", cycle*3+j))
		}

		held = priorityNext(t, held, ch, fmt.Sprintf("L%d", cycle))
	}

	cancel()

	for i := 0; i < 24; i++ {
		if r := receive(t, ch); r.p != nil || !errors.Is(r.err, context.Canceled) {
			t.Fatal(r)
		}
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityEmptyGroupsDoNotAccumulateCredit(t *testing.T) {
	cfg := priorityConfig(1)
	cfg.Priority.Groups[0].Turns = 2
	cfg.Priority.Groups[1].Turns = 1
	a := newPriority(t, cfg)
	held := acquire(t, a, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 12)

	for i := 0; i < 4; i++ {
		priorityEnqueue(ctx, t, a, ch, fmt.Sprintf("L%d", i), "L", 1, i+1)
	}

	for i := 0; i < 3; i++ {
		held = priorityNext(t, held, ch, fmt.Sprintf("L%d", i))
	}

	for i, tenant := range []string{"A", "A", "A", "M"} {
		priorityEnqueue(ctx, t, a, ch, fmt.Sprintf("new%d", i), tenant, 1, i+2)
	}

	for _, want := range []string{"new0", "new1", "new3", "L3", "new2"} {
		held = priorityNext(t, held, ch, want)
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityProtectedCancellationConsumesBothTurns(t *testing.T) {
	cfg := priorityConfig(5)
	cfg.Priority.Groups[0].Turns = 2
	cfg.Priority.Groups[1].Turns = 1
	a := newPriority(t, cfg)
	bulk := acquireN(t, a, "holder", 4)
	single := acquireN(t, a, "holder", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selected, cancelSelected := context.WithCancel(ctx)
	other, cancelOther := context.WithCancel(ctx)
	defer cancelSelected()
	defer cancelOther()
	ch := make(chan result, 4)
	priorityEnqueue(selected, t, a, ch, "A0", "A", 4, 1)
	priorityEnqueue(ctx, t, a, ch, "A1", "A", 4, 2)
	priorityEnqueue(other, t, a, ch, "B0", "B", 4, 3)
	priorityEnqueue(ctx, t, a, ch, "L0", "L", 4, 4)
	single.Release() // Select A0 and rotate A behind B with one free token.
	cancelSelected()
	if r := receive(t, ch); r.id != "A0" || !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}

	cancelOther() // B0 was selected by the second high slot; low is next.
	if r := receive(t, ch); r.id != "B0" || !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}
	if s := a.Stats(); s.InUse != 4 || s.Queued != 2 || s.WaitingTenants != 2 {
		t.Fatal(s)
	}

	held := priorityNext(t, bulk, ch, "L0")
	held = priorityNext(t, held, ch, "A1")
	held.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityUnselectedCancellationDoesNotConsumeTurn(t *testing.T) {
	cfg := priorityConfig(1)
	cfg.Priority.Groups[0].Turns = 2
	cfg.Priority.Groups[1].Turns = 1
	a := newPriority(t, cfg)
	held := acquire(t, a, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	middle, cancelMiddle := context.WithCancel(ctx)
	defer cancelMiddle()
	ch := make(chan result, 5)
	priorityEnqueue(ctx, t, a, ch, "A0", "A", 1, 1)
	priorityEnqueue(middle, t, a, ch, "A1", "A", 1, 2)
	priorityEnqueue(ctx, t, a, ch, "A2", "A", 1, 3)
	priorityEnqueue(ctx, t, a, ch, "B0", "B", 1, 4)
	priorityEnqueue(ctx, t, a, ch, "L0", "L", 1, 5)
	cancelMiddle() // Capacity is full, so no request has been selected yet.
	if r := receive(t, ch); r.id != "A1" || !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}

	for _, want := range []string{"A0", "B0", "L0", "A2"} {
		held = priorityNext(t, held, ch, want)
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityQueueIsolationAndErrorPrecedence(t *testing.T) {
	cfg := priorityConfig(1)
	cfg.MaxQueued, cfg.MaxQueuedPerTenant = 4, 1
	cfg.Priority.Groups[0].MaxQueued = 3
	cfg.Priority.Groups[1].MaxQueued = 0
	cfg.Priority.Groups[2].MaxQueued = 1
	cfg.Priority.TenantGroups["C"] = "high"
	a := newPriority(t, cfg)
	held := acquire(t, a, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 4)
	wantError := func(tenant string, want error) {
		t.Helper()
		before := a.Stats()
		if p, err := a.AcquireFor(ctx, tenant); p != nil || !errors.Is(err, want) {
			t.Fatalf("tenant=%s: permit=%v error=%v want=%v", tenant, p, err, want)
		}
		if after := a.Stats(); after != before {
			t.Fatalf("rejection changed accounting: before=%+v after=%+v", before, after)
		}
	}
	priorityEnqueue(ctx, t, a, ch, "A", "A", 1, 1)
	wantError("A", airlock.ErrTenantQueueFull)
	priorityEnqueue(ctx, t, a, ch, "B", "B", 1, 2)
	priorityEnqueue(ctx, t, a, ch, "C", "C", 1, 3)
	wantError("A", airlock.ErrGroupQueueFull)
	wantError("M", airlock.ErrGroupQueueFull)
	priorityEnqueue(ctx, t, a, ch, "L", "L", 1, 4) // High cannot consume low's reserved entry.
	wantError("A", airlock.ErrQueueFull)

	for _, want := range []string{"A", "B", "C", "L"} {
		held = priorityNext(t, held, ch, want)
	}

	held.Release()
	// A group with no queue allowance can still use the entire idle capacity.
	acquire(t, a, "M").Release()
	p, err := a.TryAcquireFor("M")
	if err != nil {
		t.Fatal(err)
	}

	p.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityUnallocatedQueueBudgetIsNotBorrowed(t *testing.T) {
	cfg := priorityConfig(1)
	cfg.MaxQueued = 5
	cfg.MaxQueuedPerTenant = 5

	for i := range cfg.Priority.Groups {
		cfg.Priority.Groups[i].MaxQueued = 1
	}

	a := newPriority(t, cfg)
	held := acquire(t, a, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 1)
	priorityEnqueue(ctx, t, a, ch, "A", "A", 1, 1)
	if p, err := a.AcquireFor(ctx, "B"); p != nil || !errors.Is(err, airlock.ErrGroupQueueFull) {
		t.Fatalf("unused global budget must not enlarge group budget: %v %v", p, err)
	}

	held = priorityNext(t, held, ch, "A")
	held.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityProtectedLowCannotBeBypassed(t *testing.T) {
	a := newPriority(t, priorityConfig(5))
	held := acquireN(t, a, "holder", 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 2)
	priorityEnqueue(ctx, t, a, ch, "L", "L", 5, 1) // Low selected before high arrives.
	before := a.Stats()

	for _, try := range []func() (*airlock.Permit, error){
		a.TryAcquire,
		func() (*airlock.Permit, error) { return a.TryAcquireFor("A") },
		func() (*airlock.Permit, error) { return a.TryAcquireN(1) },
		func() (*airlock.Permit, error) { return a.TryAcquireNFor("A", 1) },
	} {
		if p, err := try(); p != nil || !errors.Is(err, airlock.ErrWouldBlock) {
			t.Fatalf("Try bypassed protected low: %v %v", p, err)
		}
	}

	if a.Stats() != before {
		t.Fatal("Try changed queue or capacity accounting")
	}

	priorityEnqueue(ctx, t, a, ch, "A", "A", 1, 2)
	held = priorityNext(t, held, ch, "L")
	held = priorityNext(t, held, ch, "A")
	held.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityFastPathsAndTryFailuresPreserveCursor(t *testing.T) {
	cfg := priorityConfig(1)
	cfg.Priority.Groups[0].Turns = 2
	cfg.Priority.Groups[1].Turns = 1
	a := newPriority(t, cfg)
	held := acquire(t, a, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 3)
	priorityEnqueue(ctx, t, a, ch, "first", "A", 1, 1)
	held = priorityNext(t, held, ch, "first") // Consume only high slot zero.
	held.Release()

	for _, tenant := range []string{"A", "M", "L"} {
		acquire(t, a, tenant).Release()
		p, err := a.TryAcquireFor(tenant)
		if err != nil {
			t.Fatal(err)
		}
		if q, err := a.TryAcquireFor("B"); q != nil || !errors.Is(err, airlock.ErrWouldBlock) {
			t.Fatalf("full-capacity Try=%v,%v", q, err)
		}

		p.Release()
	}

	held = acquire(t, a, "holder")
	priorityEnqueue(ctx, t, a, ch, "A0", "A", 1, 1)
	priorityEnqueue(ctx, t, a, ch, "A1", "A", 1, 2)
	priorityEnqueue(ctx, t, a, ch, "L0", "L", 1, 3)

	// The saved cursor is high slot one: only one H selection precedes L.
	for _, want := range []string{"A0", "L0", "A1"} {
		held = priorityNext(t, held, ch, want)
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityCancelledTenantRejoinsGroupTail(t *testing.T) {
	cfg := priorityConfig(1)
	cfg.Priority.TenantGroups["C"] = "high"
	a := newPriority(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for wave := 0; wave < 3; wave++ {
		held := acquire(t, a, "holder")
		cancelled, cancelTenant := context.WithCancel(ctx)
		ch := make(chan result, 4)
		priorityEnqueue(cancelled, t, a, ch, "A0", "A", 1, 1)
		priorityEnqueue(cancelled, t, a, ch, "A1", "A", 1, 2)
		priorityEnqueue(ctx, t, a, ch, "B0", "B", 1, 3)
		priorityEnqueue(ctx, t, a, ch, "C0", "C", 1, 4)
		cancelTenant()

		for i := 0; i < 2; i++ {
			if r := receive(t, ch); r.p != nil || !errors.Is(r.err, context.Canceled) {
				t.Fatal(r)
			}
		}

		if s := a.Stats(); s.WaitingTenants != 2 || s.Queued != 2 {
			t.Fatal(s)
		}

		priorityEnqueue(ctx, t, a, ch, "A2", "A", 1, 3)

		for _, want := range []string{"B0", "C0", "A2"} {
			held = priorityNext(t, held, ch, want)
		}

		held.Release()
		assertWeightedIdle(t, a)
	}
}

func TestPriorityGrantWinsLaterCancellation(t *testing.T) {
	a := newPriority(t, priorityConfig(3))
	held := acquireN(t, a, "holder", 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 1)
	priorityEnqueue(ctx, t, a, ch, "L", "L", 3, 1)
	held.Release() // Synchronously reserves all tokens before notifying L.
	cancel()
	r := receive(t, ch)
	if r.err != nil || r.p == nil {
		t.Fatal(r)
	}

	t.Cleanup(r.p.Release)
	if s := a.Stats(); s.InUse != 3 || s.ActivePermits != 1 || s.Queued != 0 {
		t.Fatal(s)
	}

	r.p.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityGroupBudgetsRecoverAfterGrantAndCancel(t *testing.T) {
	cfg := priorityConfig(1)
	cfg.MaxQueued, cfg.MaxQueuedPerTenant = 2, 1
	cfg.Priority.Groups[0].MaxQueued = 1
	cfg.Priority.Groups[1].MaxQueued = 0
	cfg.Priority.Groups[2].MaxQueued = 1
	a := newPriority(t, cfg)
	held := acquire(t, a, "holder")

	for wave := 0; wave < 6; wave++ {
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan result, 2)
		// Each group's single waiting entry must be reusable after either exit.
		priorityEnqueue(ctx, t, a, ch, "A", "A", 1, 1)
		priorityEnqueue(ctx, t, a, ch, "L", "L", 1, 2)
		if wave%2 == 0 {
			for _, want := range []string{"A", "L"} {
				held = priorityNext(t, held, ch, want)
			}

			cancel()
		} else {
			cancel()

			for i := 0; i < 2; i++ {
				if r := receive(t, ch); r.p != nil || !errors.Is(r.err, context.Canceled) {
					t.Fatal(r)
				}
			}
		}

		if s := a.Stats(); s.InUse != 1 || s.Queued != 0 || s.WaitingTenants != 0 {
			t.Fatal(s)
		}
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestPriorityCancelGrantRaceAndReclamation(t *testing.T) {
	for round := 0; round < 50; round++ {
		a := newPriority(t, priorityConfig(5))
		held := acquireN(t, a, "holder", 5)
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan result, 3)

		for i, tenant := range []string{"A", "M", "L"} {
			priorityEnqueue(ctx, t, a, ch, tenant, tenant, 5, i+1)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			cancel()
		}()
		go func() {
			defer wg.Done()
			held.Release()
		}()
		wg.Wait()

		for i := 0; i < 3; i++ {
			r := receive(t, ch)
			if r.err == nil && r.p != nil {
				for j := 0; j < 4; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						r.p.Release()
					}()
				}

				wg.Wait()
			} else if r.p != nil || !errors.Is(r.err, context.Canceled) {
				t.Fatal(r)
			}
		}

		assertWeightedIdle(t, a)
	}
}

func TestPriorityMixedWeightConcurrentCapacity(t *testing.T) {
	const capacity, workers = 11, 120
	cfg := priorityConfig(capacity)
	cfg.MaxQueued, cfg.MaxQueuedPerTenant = workers, workers

	for i := range cfg.Priority.Groups {
		cfg.Priority.Groups[i].MaxQueued = workers / 3
	}

	for i := 0; i < workers; i++ {
		cfg.Priority.TenantGroups[fmt.Sprint(i)] = cfg.Priority.Groups[i%3].Name
	}

	a := newPriority(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var active atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			weight := i%capacity + 1
			p, err := a.AcquireNFor(ctx, fmt.Sprint(i), weight)
			if err != nil {
				t.Error(err)
				return
			}

			if got := active.Add(int64(weight)); got > capacity {
				t.Errorf("held %d tokens exceeds capacity %d", got, capacity)
			}

			runtime.Gosched()
			active.Add(-int64(weight))
			p.Release()
		}(i)
	}

	wg.Wait()
	if active.Load() != 0 {
		t.Fatal("work accounting leaked")
	}

	assertWeightedIdle(t, a)
}

func TestPriorityFiniteQueuesMatchReferenceSelection(t *testing.T) {
	// A bounded, deterministic random workload exercises group skipping, unequal
	// weights, tenant exhaustion and FIFO. Every weight exceeds half capacity,
	// making each observed result an unambiguous grant barrier, not a race among
	// goroutines that were admitted together. The slice model computes the full
	// order before the implementation is allowed to release its initial holder.
	for seed := int64(0); seed < 12; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			cfg := priorityConfig(5)
			a := newPriority(t, cfg)
			held := acquireN(t, a, "holder", 5)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch := make(chan result, 24)
			queues := map[string][]string{}
			rotations := [][]string{{}, {}, {}}
			tenants := []string{"A", "B", "M", "L", "unmapped"}
			group := map[string]int{"A": 0, "B": 0, "M": 1, "L": 2, "unmapped": 2}

			for i := 0; i < 24; i++ {
				tenant := tenants[rng.Intn(len(tenants))]
				id := fmt.Sprintf("%s/%d", tenant, i)
				if len(queues[tenant]) == 0 {
					rotations[group[tenant]] = append(rotations[group[tenant]], tenant)
				}

				queues[tenant] = append(queues[tenant], id)
				priorityEnqueue(ctx, t, a, ch, id, tenant, 3+rng.Intn(3), i+1)
			}

			slots := []int{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2}
			cursor := 0
			var want []string

			for len(want) < 24 {
				g := slots[cursor]
				cursor = (cursor + 1) % len(slots)
				if len(rotations[g]) == 0 {
					continue
				}

				tenant := rotations[g][0]
				rotations[g] = rotations[g][1:]
				want = append(want, queues[tenant][0])
				queues[tenant] = queues[tenant][1:]
				if len(queues[tenant]) != 0 {
					rotations[g] = append(rotations[g], tenant)
				}
			}

			for _, id := range want {
				held = priorityNext(t, held, ch, id)
			}

			held.Release()
			assertWeightedIdle(t, a)
		})
	}
}

func TestPriorityResizeKeepsProtectedHeadAndQueueBudgets(t *testing.T) {
	a := newPriority(t, priorityConfig(4))
	held := acquireN(t, a, "holder", 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 3)
	priorityEnqueue(ctx, t, a, ch, "low", "L", 4, 1)
	priorityEnqueue(ctx, t, a, ch, "high", "A", 1, 2)
	if err := a.Resize(5); err != nil {
		t.Fatal(err)
	}
	if s := a.Stats(); s.InUse != 3 || s.Queued != 2 {
		t.Fatal(s)
	}
	if err := a.Resize(3); err != nil {
		t.Fatal(err)
	}

	r := receive(t, ch)
	if r.id != "low" || !errors.Is(r.err, airlock.ErrWeightExceedsCapacity) {
		t.Fatalf("%+v", r)
	}

	held.Release()
	r = receive(t, ch)
	if r.id != "high" || r.err != nil {
		t.Fatalf("%+v", r)
	}

	r.p.Release()
	// The rejected group's queue and tenant accounting can be reused.
	p := acquireN(t, a, "L", 3)
	p.Release()
	assertWeightedIdle(t, a)
}

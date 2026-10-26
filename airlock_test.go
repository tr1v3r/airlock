package airlock_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/tr1v3r/airlock"
	"runtime"
	"sync"
	"testing"
	"time"
)

func newAirlock(t *testing.T, capacity, total, tenant int) *airlock.Airlock {
	t.Helper()
	g, err := airlock.New(airlock.Config{Capacity: capacity, MaxQueued: total, MaxQueuedPerTenant: tenant})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func acquire(t *testing.T, g *airlock.Airlock, tenant string) *airlock.Permit {
	t.Helper()
	p, err := g.AcquireFor(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Release)
	return p
}

func queued(t *testing.T, g *airlock.Airlock, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for g.Stats().Queued != n {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for queued=%d, got %+v", n, g.Stats())
		}
		runtime.Gosched()
	}
}

type result struct {
	id  string
	p   *airlock.Permit
	err error
}

func receive(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("acquisition did not finish")
		return result{}
	}
}

func TestRoundRobinFIFOAndNoBarging(t *testing.T) {
	g := newAirlock(t, 1, 20, 10)
	held := acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 10)
	for i, id := range []string{"A1", "A2", "A3", "B1", "B2", "C1"} {
		go func(id string) { p, err := g.AcquireFor(ctx, id[:1]); ch <- result{id, p, err} }(id)
		queued(t, g, i+1)
	}
	for _, want := range []string{"A1", "B1", "C1", "A2", "B2", "A3"} {
		held.Release()
		r := receive(t, ch)
		if r.err != nil || r.id != want {
			t.Fatalf("got %s/%v, want %s", r.id, r.err, want)
		}
		held = r.p
		t.Cleanup(held.Release)
	}
	held.Release()
	if got := g.Stats(); got.InUse != 0 || got.Queued != 0 || got.WaitingTenants != 0 {
		t.Fatal(got)
	}
}

func TestLimitsCancellationAndReuse(t *testing.T) {
	g := newAirlock(t, 1, 3, 2)
	held := acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 3)
	for i, tenant := range []string{"A", "A", "B"} {
		go func(tenant string) { p, err := g.AcquireFor(ctx, tenant); ch <- result{tenant, p, err} }(tenant)
		queued(t, g, i+1)
		if i == 1 {
			if _, err := g.AcquireFor(ctx, "A"); !errors.Is(err, airlock.ErrTenantQueueFull) {
				t.Fatal(err)
			}
		}
	}
	if _, err := g.AcquireFor(ctx, "A"); !errors.Is(err, airlock.ErrQueueFull) {
		t.Fatal(err)
	}
	cancel()
	for i := 0; i < 3; i++ {
		r := receive(t, ch)
		if !errors.Is(r.err, context.Canceled) {
			t.Fatal(r.err)
		}
	}
	if got := g.Stats(); got.InUse != 1 || got.Queued != 0 || got.WaitingTenants != 0 {
		t.Fatal(got)
	}
	held.Release()
	acquire(t, g, "new").Release()
}

func TestZeroQueueAndValidation(t *testing.T) {
	for _, cfg := range []airlock.Config{{}, {Capacity: -1}, {Capacity: 1, MaxQueued: -1}, {Capacity: 1, MaxQueuedPerTenant: -1}, {Capacity: 1, MaxQueued: 1, MaxQueuedPerTenant: 2}} {
		if _, err := airlock.New(cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	g := newAirlock(t, 1, 0, 0)
	if _, err := g.AcquireFor(context.Background(), ""); !errors.Is(err, airlock.ErrInvalidTenant) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.AcquireFor(ctx, "A"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p := acquire(t, g, "A")
	if _, err := g.AcquireFor(context.Background(), "B"); !errors.Is(err, airlock.ErrQueueFull) {
		t.Fatal(err)
	}
	p.Release()
	defer func() {
		if recover() == nil {
			t.Error("nil context did not panic")
		}
	}()
	_, _ = g.AcquireFor(nil, "A") //nolint:staticcheck // SA1012: verify the documented nil-context panic.
}

func TestCancelGrantRaceAndDuplicateRelease(t *testing.T) {
	for i := 0; i < 200; i++ {
		g := newAirlock(t, 1, 1, 1)
		held := acquire(t, g, "holder")
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan result, 1)
		go func() { p, err := g.AcquireFor(ctx, "A"); ch <- result{p: p, err: err} }()
		queued(t, g, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancel() }()
		go func() { defer wg.Done(); held.Release() }()
		r := receive(t, ch)
		wg.Wait()
		if r.err == nil {
			for j := 0; j < 8; j++ {
				wg.Add(1)
				go func() { defer wg.Done(); r.p.Release() }()
			}
			wg.Wait()
		} else if !errors.Is(r.err, context.Canceled) {
			t.Fatal(r.err)
		}
		if got := g.Stats(); got.InUse != 0 || got.Queued != 0 || got.WaitingTenants != 0 {
			t.Fatal(got)
		}
	}
}

func TestConcurrentCapacityAndTenantReclamation(t *testing.T) {
	g := newAirlock(t, 8, 512, 512)
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p, err := g.AcquireFor(context.Background(), fmt.Sprint(i))
			if err != nil {
				t.Error(err)
				return
			}
			s := g.Stats()
			if s.InUse < 1 || s.InUse > 8 || s.WaitingTenants > s.Queued {
				t.Error(s)
			}
			runtime.Gosched()
			p.Release()
		}(i)
	}
	wg.Wait()
	if s := g.Stats(); s.InUse != 0 || s.Queued != 0 || s.WaitingTenants != 0 {
		t.Fatal(s)
	}
}

func TestSingleTenantUsesAllSlots(t *testing.T) {
	g := newAirlock(t, 8, 0, 0)
	for i := 0; i < 8; i++ {
		acquire(t, g, "A")
	}
	if s := g.Stats(); s.InUse != 8 {
		t.Fatal(s)
	}
}

func TestGrantedSlotCannotBeStolen(t *testing.T) {
	g := newAirlock(t, 1, 1, 1)
	held := acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 1)
	go func() { p, err := g.AcquireFor(ctx, "A"); ch <- result{p: p, err: err} }()
	queued(t, g, 1)
	held.Release() // reserves the slot for A before waking its goroutine
	cancel()       // cancellation cannot revoke that reservation
	r := receive(t, ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(r.p.Release)
	ctxB, cancelB := context.WithCancel(context.Background())
	go func() { p, err := g.AcquireFor(ctxB, "B"); ch <- result{p: p, err: err} }()
	queued(t, g, 1)
	cancelB()
	if r := receive(t, ch); !errors.Is(r.err, context.Canceled) {
		t.Fatal(r.err)
	}
	if s := g.Stats(); s.InUse != 1 || s.Queued != 0 {
		t.Fatal(s)
	}
	r.p.Release()
}

func TestAirlockErrorPrecedence(t *testing.T) {
	g := newAirlock(t, 1, 1, 1)
	acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := make(chan result, 1)
	go func() { p, err := g.AcquireFor(ctx, "A"); ch <- result{p: p, err: err} }()
	queued(t, g, 1)
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	for _, tc := range []struct {
		ctx    context.Context
		tenant string
		want   error
	}{
		{cancelled, "", airlock.ErrInvalidTenant},
		{cancelled, "A", context.Canceled},
		{context.Background(), "A", airlock.ErrQueueFull},
		{context.Background(), "B", airlock.ErrQueueFull},
	} {
		p, err := g.AcquireFor(tc.ctx, tc.tenant)
		if p != nil || !errors.Is(err, tc.want) {
			t.Fatalf("permit=%v error=%v want=%v", p, err, tc.want)
		}
	}
	cancel()
	if r := receive(t, ch); !errors.Is(r.err, context.Canceled) {
		t.Fatal(r.err)
	}
}

func TestAirlockZeroTenantQueueAndInvalidConfig(t *testing.T) {
	g := newAirlock(t, 1, 10, 0)
	p := acquire(t, g, "A")
	if _, err := g.AcquireFor(context.Background(), "B"); !errors.Is(err, airlock.ErrTenantQueueFull) {
		t.Fatal(err)
	}
	p.Release()
	acquire(t, g, "B").Release()
	if _, err := airlock.New(airlock.Config{}); !errors.Is(err, airlock.ErrInvalidConfig) {
		t.Fatal(err)
	}
}

func TestAirlockCancelledTenantRejoinsAtTail(t *testing.T) {
	g := newAirlock(t, 1, 10, 10)
	held := acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 10)
	for i, id := range []string{"A1", "A2", "B1", "C1"} {
		requestCtx := context.Background()
		if id[0] == 'A' {
			requestCtx = ctx
		}
		go func(id string, requestCtx context.Context) {
			p, err := g.AcquireFor(requestCtx, id[:1])
			ch <- result{id, p, err}
		}(id, requestCtx)
		queued(t, g, i+1)
	}
	cancel()
	for i := 0; i < 2; i++ {
		r := receive(t, ch)
		if !errors.Is(r.err, context.Canceled) {
			t.Fatal(r)
		}
	}
	queued(t, g, 2)
	go func() { p, err := g.AcquireFor(context.Background(), "A"); ch <- result{"A3", p, err} }()
	queued(t, g, 3)
	for _, want := range []string{"B1", "C1", "A3"} {
		held.Release()
		r := receive(t, ch)
		if r.err != nil || r.id != want {
			t.Fatalf("got %+v want %s", r, want)
		}
		held = r.p
		t.Cleanup(held.Release)
	}
	held.Release()
}

func TestAirlockCancelMiddleRequest(t *testing.T) {
	g := newAirlock(t, 1, 10, 10)
	held := acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 4)
	for i, id := range []string{"A1", "A2", "A3", "B1"} {
		requestCtx := context.Background()
		if id == "A2" {
			requestCtx = ctx
		}
		go func(id string, requestCtx context.Context) {
			p, err := g.AcquireFor(requestCtx, id[:1])
			ch <- result{id, p, err}
		}(id, requestCtx)
		queued(t, g, i+1)
	}
	cancel()
	r := receive(t, ch)
	if r.id != "A2" || !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}
	for _, want := range []string{"A1", "B1", "A3"} {
		held.Release()
		r := receive(t, ch)
		if r.err != nil || r.id != want {
			t.Fatalf("got %+v want %s", r, want)
		}
		held = r.p
		t.Cleanup(held.Release)
	}
	held.Release()
}

func TestAirlockMultipleSlotsAndChurn(t *testing.T) {
	g := newAirlock(t, 4, 32, 4)
	for wave := 0; wave < 30; wave++ {
		var held []*airlock.Permit
		for i := 0; i < 4; i++ {
			held = append(held, acquire(t, g, "holder"))
		}
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan result, 16)
		for i := 0; i < 16; i++ {
			id := fmt.Sprintf("%d/%d", wave, i)
			go func() { p, err := g.AcquireFor(ctx, id); ch <- result{id, p, err} }()
			queued(t, g, i+1)
		}
		var wg sync.WaitGroup
		for _, p := range held {
			wg.Add(1)
			go func() { defer wg.Done(); p.Release() }()
		}
		wg.Wait()
		var granted []*airlock.Permit
		for i := 0; i < 4; i++ {
			r := receive(t, ch)
			if r.err != nil {
				t.Fatal(r.err)
			}
			granted = append(granted, r.p)
		}
		if s := g.Stats(); s.InUse != 4 || s.Queued != 12 || s.WaitingTenants != 12 {
			t.Fatal(s)
		}
		cancel()
		for i := 0; i < 12; i++ {
			r := receive(t, ch)
			if !errors.Is(r.err, context.Canceled) {
				t.Fatal(r)
			}
		}
		for _, p := range granted {
			p.Release()
		}
		if s := g.Stats(); s.InUse != 0 || s.Queued != 0 || s.WaitingTenants != 0 {
			t.Fatal(s)
		}
	}
}

func TestAirlockDeadlineAndPermitZeroValue(t *testing.T) {
	g := newAirlock(t, 1, 1, 1)
	held := acquire(t, g, "A")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if p, err := g.AcquireFor(ctx, "B"); p != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%v %v", p, err)
	}
	if s := g.Stats(); s.InUse != 1 || s.Queued != 0 || s.WaitingTenants != 0 {
		t.Fatal(s)
	}
	held.Release()
	var zero airlock.Permit
	zero.Release()
	zero.Release()
	var absent *airlock.Permit
	absent.Release()
}

func ExampleAirlock() {
	g, err := airlock.New(airlock.Config{Capacity: 2, MaxQueued: 8, MaxQueuedPerTenant: 4})
	if err != nil {
		panic(err)
	}
	p, err := g.AcquireFor(context.Background(), "tenant-a")
	if err != nil {
		panic(err)
	}
	fmt.Println(g.Stats().InUse)
	p.Release()
	fmt.Println(g.Stats().InUse)
	// Output:
	// 1
	// 0
}

func TestDefaultTenantFastPathAndNoWaiting(t *testing.T) {
	a := newAirlock(t, 1, 0, 0)
	p, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Release)
	if p, err := a.Acquire(context.Background()); p != nil || !errors.Is(err, airlock.ErrQueueFull) {
		t.Fatalf("%v %v", p, err)
	}
	p.Release()
	// The released default slot can be acquired again.
	p, err = a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Release()
	if s := a.Stats(); s.InUse != 0 || s.Queued != 0 || s.WaitingTenants != 0 {
		t.Fatal(s)
	}
}

func TestAcquireForValidation(t *testing.T) {
	a := newAirlock(t, 1, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, c := range []context.Context{context.Background(), ctx} {
		p, err := a.AcquireFor(c, "")
		if p != nil || !errors.Is(err, airlock.ErrInvalidTenant) {
			t.Fatalf("%v %v", p, err)
		}
	}
	if p, err := a.Acquire(ctx); p != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("%v %v", p, err)
	}
	defer func() {
		if recover() == nil {
			t.Error("nil context did not panic")
		}
	}()
	_, _ = a.Acquire(nil) //nolint:staticcheck // SA1012: verify the documented nil-context panic.
}

func TestDefaultTenantQueueLimitAndCancellation(t *testing.T) {
	a := newAirlock(t, 1, 3, 1)
	held := acquire(t, a, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 2)
	go func() { p, err := a.Acquire(ctx); ch <- result{p: p, err: err} }()
	queued(t, a, 1)
	if _, err := a.Acquire(ctx); !errors.Is(err, airlock.ErrTenantQueueFull) {
		t.Fatal(err)
	}
	// A named "default" tenant cannot collide with the internal default tenant.
	go func() { p, err := a.AcquireFor(ctx, "default"); ch <- result{p: p, err: err} }()
	queued(t, a, 2)
	if s := a.Stats(); s.WaitingTenants != 2 {
		t.Fatal(s)
	}
	cancel()
	for i := 0; i < 2; i++ {
		if r := receive(t, ch); !errors.Is(r.err, context.Canceled) {
			t.Fatal(r.err)
		}
	}
	held.Release()
	if s := a.Stats(); s.InUse != 0 || s.Queued != 0 || s.WaitingTenants != 0 {
		t.Fatal(s)
	}
}

func TestDefaultTenantParticipatesInRotation(t *testing.T) {
	a := newAirlock(t, 1, 4, 2)
	held := acquire(t, a, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 4)
	for i, id := range []string{"D1", "D2", "A1", "A2"} {
		go func(id string) {
			var p *airlock.Permit
			var err error
			if id[0] == 'A' {
				p, err = a.AcquireFor(ctx, "A")
			} else {
				p, err = a.Acquire(ctx)
			}
			ch <- result{id, p, err}
		}(id)
		queued(t, a, i+1)
	}
	for _, want := range []string{"D1", "A1", "D2", "A2"} {
		held.Release()
		r := receive(t, ch)
		if r.err != nil || r.id != want {
			t.Fatalf("got %+v, want %s", r, want)
		}
		held = r.p
		t.Cleanup(held.Release)
	}
	held.Release()
}

func ExampleAirlock_Acquire() {
	lock, err := airlock.New(airlock.Config{Capacity: 2})
	if err != nil {
		panic(err)
	}
	permit, err := lock.Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	defer permit.Release()
	fmt.Println("hello, airlock")
	// Output: hello, airlock
}

// Keep the two entry points usable through explicit, non-variadic interfaces.
var _ interface {
	Acquire(context.Context) (*airlock.Permit, error)
	AcquireFor(context.Context, string) (*airlock.Permit, error)
} = (*airlock.Airlock)(nil)

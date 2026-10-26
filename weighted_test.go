package airlock_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func acquireN(t *testing.T, a *airlock.Airlock, tenant string, n int) *airlock.Permit {
	t.Helper()
	p, err := a.AcquireNFor(context.Background(), tenant, n)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(p.Release)
	return p
}

func assertWeightedIdle(t *testing.T, a *airlock.Airlock) {
	t.Helper()
	if s := a.Stats(); s.InUse != 0 || s.ActivePermits != 0 || s.Queued != 0 || s.WaitingTenants != 0 {
		t.Fatalf("expected all permits and queues reclaimed, got %+v", s)
	}
}

func TestWeightedValidationAndErrorPrecedence(t *testing.T) {
	a := newAirlock(t, 3, 0, 0)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name   string
		ctx    context.Context
		tenant string
		n      int
		want   error
	}{
		{"empty tenant before weight", cancelled, "", 0, airlock.ErrInvalidTenant},
		{"empty tenant before oversize", cancelled, "", 4, airlock.ErrInvalidTenant},
		{"zero before cancellation", cancelled, "A", 0, airlock.ErrInvalidWeight},
		{"negative before cancellation", cancelled, "A", -1, airlock.ErrInvalidWeight},
		{"oversize before cancellation", cancelled, "A", 4, airlock.ErrWeightExceedsCapacity},
		{"valid cancelled", cancelled, "A", 3, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := a.AcquireNFor(tc.ctx, tc.tenant, tc.n)
			if p != nil || !errors.Is(err, tc.want) {
				t.Fatalf("permit=%v error=%v want=%v", p, err, tc.want)
			}

			assertWeightedIdle(t, a)
		})
	}

	for _, n := range []int{-1, 0, 4} {
		want := airlock.ErrInvalidWeight
		if n > 3 {
			want = airlock.ErrWeightExceedsCapacity
		}

		if p, err := a.AcquireN(cancelled, n); p != nil || !errors.Is(err, want) {
			t.Fatalf("default tenant weight=%d: permit=%v error=%v want=%v", n, p, err, want)
		}
	}

	for _, named := range []bool{false, true} {
		t.Run(fmt.Sprintf("nil context named=%v", named), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("nil context must panic before tenant or weight validation")
				}
			}()
			if named {
				_, _ = a.AcquireNFor(nil, "", 0) //nolint:staticcheck // SA1012: test the nil-context contract.
			} else {
				_, _ = a.AcquireN(nil, 0) //nolint:staticcheck // SA1012: test the nil-context contract.
			}
		})
	}

	// A full queue must not mask invalid weights or a cancelled context.
	held := acquireN(t, a, "holder", 3)

	for _, tc := range []struct {
		n    int
		want error
	}{{0, airlock.ErrInvalidWeight}, {4, airlock.ErrWeightExceedsCapacity}, {1, context.Canceled}} {
		if p, err := a.AcquireN(cancelled, tc.n); p != nil || !errors.Is(err, tc.want) {
			t.Fatalf("permit=%v error=%v want=%v", p, err, tc.want)
		}
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestWeightedFastPathUnitsAndWholeRelease(t *testing.T) {
	a := newAirlock(t, 5, 0, 0)
	p, err := a.AcquireN(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(p.Release)
	q := acquireN(t, a, "A", 2)
	if s := a.Stats(); s.InUse != 5 || s.ActivePermits != 2 || s.Queued != 0 {
		t.Fatal(s)
	}
	if p, err := a.AcquireN(context.Background(), 1); p != nil || !errors.Is(err, airlock.ErrQueueFull) {
		t.Fatalf("permit=%v error=%v", p, err)
	}

	p.Release()
	p.Release()
	if s := a.Stats(); s.InUse != 2 || s.ActivePermits != 1 {
		t.Fatal(s)
	}

	q.Release()
	assertWeightedIdle(t, a)
	// n == Capacity works without any waiting-queue allowance.
	acquireN(t, a, "all", 5).Release()
	assertWeightedIdle(t, a)
}

func TestWeightedAtomicAcquisitionAndProtectedHead(t *testing.T) {
	a := newAirlock(t, 5, 2, 1)
	first := acquireN(t, a, "holder", 2)
	second := acquireN(t, a, "holder", 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	large, small := make(chan result, 1), make(chan result, 1)
	go func() {
		p, err := a.AcquireNFor(ctx, "large", 4)
		large <- result{p: p, err: err}
	}()
	queued(t, a, 1)
	go func() {
		p, err := a.AcquireNFor(ctx, "small", 1)
		small <- result{p: p, err: err}
	}()
	queued(t, a, 2)
	if s := a.Stats(); s.InUse != 4 || s.ActivePermits != 2 || s.WaitingTenants != 2 {
		t.Fatalf("a pending request must not acquire any tokens: %+v", s)
	}

	first.Release()
	// There are three free tokens, but neither the protected n=4 request nor
	// the later n=1 request may be granted yet. Release dispatches synchronously.
	if s := a.Stats(); s.InUse != 2 || s.ActivePermits != 1 || s.Queued != 2 {
		t.Fatalf("small request bypassed protected request, or partial grant: %+v", s)
	}

	second.Release()

	for _, ch := range []<-chan result{large, small} {
		r := receive(t, ch)
		if r.err != nil || r.p == nil {
			t.Fatal(r)
		}

		t.Cleanup(r.p.Release)
	}

	if s := a.Stats(); s.InUse != 5 || s.ActivePermits != 2 || s.Queued != 0 || s.WaitingTenants != 0 {
		t.Fatal(s)
	}
}

func TestWeightedRoundRobinUsesRequestsRatherThanTokens(t *testing.T) {
	a := newAirlock(t, 5, 6, 3)
	held := acquireN(t, a, "holder", 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 6)

	for i, request := range []struct {
		id string
		n  int
	}{{"A1", 3}, {"A2", 5}, {"A3", 4}, {"B1", 4}, {"B2", 3}, {"C1", 5}} {
		go func(id string, n int) {
			p, err := a.AcquireNFor(ctx, id[:1], n)
			ch <- result{id, p, err}
		}(request.id, request.n)
		queued(t, a, i+1)
	}

	// All weights exceed half the capacity, so there is only one outstanding
	// grant at each receive barrier. The test does not assume goroutine order.
	for _, want := range []string{"A1", "B1", "C1", "A2", "B2", "A3"} {
		held.Release()
		r := receive(t, ch)
		if r.err != nil || r.id != want {
			t.Fatalf("got %+v, want %s", r, want)
		}

		held = r.p
		t.Cleanup(held.Release)
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestWeightedProtectedCancellationConsumesTenantTurn(t *testing.T) {
	a := newAirlock(t, 5, 4, 2)
	bulk := acquireN(t, a, "holder", 4)
	single := acquireN(t, a, "holder", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	protectedCtx, cancelProtected := context.WithCancel(ctx)
	defer cancelProtected()
	ch := make(chan result, 4)

	for i, id := range []string{"A1", "A2", "B1", "C1"} {
		requestCtx := ctx
		if id == "A1" {
			requestCtx = protectedCtx
		}

		go func(id string, requestCtx context.Context) {
			p, err := a.AcquireNFor(requestCtx, id[:1], 4)
			ch <- result{id, p, err}
		}(id, requestCtx)
		queued(t, a, i+1)
	}

	single.Release() // Select A1 with only one free token, moving A behind B/C.
	cancelProtected()
	if r := receive(t, ch); r.id != "A1" || r.p != nil || !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}
	if s := a.Stats(); s.InUse != 4 || s.ActivePermits != 1 || s.Queued != 3 || s.WaitingTenants != 3 {
		t.Fatal(s)
	}

	held := bulk

	for _, want := range []string{"B1", "C1", "A2"} {
		held.Release()
		r := receive(t, ch)
		if r.err != nil || r.id != want {
			t.Fatalf("cancelled protected turn was rewound: got %+v, want %s", r, want)
		}

		held = r.p
		t.Cleanup(held.Release)
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestWeightedQueueLimitsCountRequestsAndDefaultTenantIsDistinct(t *testing.T) {
	a := newAirlock(t, 5, 2, 1)
	held := acquireN(t, a, "holder", 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 2)
	go func() {
		p, err := a.AcquireN(ctx, 5)
		ch <- result{p: p, err: err}
	}()
	queued(t, a, 1)
	if p, err := a.AcquireN(ctx, 1); p != nil || !errors.Is(err, airlock.ErrTenantQueueFull) {
		t.Fatalf("permit=%v error=%v", p, err)
	}

	go func() {
		p, err := a.AcquireNFor(ctx, "default", 5)
		ch <- result{p: p, err: err}
	}()
	queued(t, a, 2)
	if s := a.Stats(); s.WaitingTenants != 2 || s.InUse != 5 || s.ActivePermits != 1 {
		t.Fatal(s)
	}
	if p, err := a.AcquireN(ctx, 1); p != nil || !errors.Is(err, airlock.ErrQueueFull) {
		t.Fatalf("global limit must precede tenant limit: permit=%v error=%v", p, err)
	}

	cancel()

	for i := 0; i < 2; i++ {
		if r := receive(t, ch); r.p != nil || !errors.Is(r.err, context.Canceled) {
			t.Fatal(r)
		}
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestWeightedCapacityArithmeticDoesNotOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	a := newAirlock(t, maxInt, 1, 1)
	held := acquireN(t, a, "holder", maxInt-1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 1)
	go func() {
		p, err := a.AcquireN(ctx, 2)
		ch <- result{p: p, err: err}
	}()
	queued(t, a, 1)
	if s := a.Stats(); s.InUse != maxInt-1 || s.ActivePermits != 1 {
		t.Fatal(s)
	}

	cancel()
	if r := receive(t, ch); r.p != nil || !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}

	held.Release()
	acquireN(t, a, "all", maxInt).Release()
	assertWeightedIdle(t, a)
}

func TestWeightedGrantWinsLaterCancellation(t *testing.T) {
	a := newAirlock(t, 3, 1, 1)
	held := acquireN(t, a, "holder", 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 1)
	go func() {
		p, err := a.AcquireN(ctx, 3)
		ch <- result{p: p, err: err}
	}()
	queued(t, a, 1)
	held.Release() // The complete three-token grant is committed before returning.
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

func TestWeightedCancelGrantRaceAndConcurrentRelease(t *testing.T) {
	for i := 0; i < 100; i++ {
		a := newAirlock(t, 3, 1, 1)
		held := acquireN(t, a, "holder", 3)
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan result, 1)
		go func() {
			p, err := a.AcquireN(ctx, 3)
			ch <- result{p: p, err: err}
		}()
		queued(t, a, 1)
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
		r := receive(t, ch)
		wg.Wait()
		if r.err == nil {
			if r.p == nil {
				t.Fatal("successful grant has no permit")
			}

			for j := 0; j < 8; j++ {
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

		assertWeightedIdle(t, a)
	}
}

func TestWeightedConcurrentCapacityAndCleanup(t *testing.T) {
	const capacity, workers = 11, 128
	a := newAirlock(t, capacity, workers, workers)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var active atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			weight := i%5 + 1
			p, err := a.AcquireNFor(ctx, fmt.Sprint(i), weight)
			if err != nil {
				t.Error(err)
				return
			}

			if n := active.Add(int64(weight)); n > capacity {
				t.Errorf("concurrent work holds %d tokens, capacity=%d", n, capacity)
			}

			s := a.Stats()
			if s.InUse < weight || s.InUse > capacity || s.ActivePermits < 1 || s.ActivePermits > s.InUse || s.WaitingTenants > s.Queued {
				t.Error(s)
			}

			runtime.Gosched()
			active.Add(-int64(weight))
			p.Release()
		}(i)
	}

	wg.Wait()
	if n := active.Load(); n != 0 {
		t.Fatalf("work accounting leaked %d tokens", n)
	}

	assertWeightedIdle(t, a)
}

var _ interface {
	AcquireN(context.Context, int) (*airlock.Permit, error)
	AcquireNFor(context.Context, string, int) (*airlock.Permit, error)
} = (*airlock.Airlock)(nil)

func TestResizeValidationAndDrain(t *testing.T) {
	a := newAirlock(t, 10, 8, 8)
	held := acquireN(t, a, "holder", 8)
	before := a.Stats()

	for _, capacity := range []int{0, -1} {
		if err := a.Resize(capacity); !errors.Is(err, airlock.ErrInvalidConfig) || a.Stats() != before {
			t.Fatalf("invalid resize: %v, %+v", err, a.Stats())
		}
	}

	if err := a.Resize(10); err != nil || a.Stats() != before {
		t.Fatalf("same capacity: %v, %+v", err, a.Stats())
	}
	if err := a.Resize(5); err != nil {
		t.Fatal(err)
	}
	if s := a.Stats(); s.Capacity != 5 || s.InUse != 8 || s.ActivePermits != 1 {
		t.Fatal(s)
	}
	if p, err := a.TryAcquire(); p != nil || !errors.Is(err, airlock.ErrWouldBlock) {
		t.Fatalf("overcommitted admission: %v %v", p, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 1)
	go func() {
		p, err := a.AcquireN(ctx, 5)
		ch <- result{p: p, err: err}
	}()
	queued(t, a, 1)
	held.Release()
	r := receive(t, ch)
	if r.err != nil || r.p == nil {
		t.Fatalf("drain: %+v", r)
	}

	r.p.Release()
	if err := a.Resize(10001); err != nil {
		t.Fatal(err)
	}

	p, err := a.TryAcquireN(10001)
	if err != nil {
		t.Fatal(err)
	}

	p.Release()
	assertWeightedIdle(t, a)
}

func TestResizePreservesProtectedHeadAndFIFO(t *testing.T) {
	a := newAirlock(t, 5, 8, 8)
	held := acquireN(t, a, "holder", 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 3)

	for i, req := range []struct {
		id     string
		weight int
	}{{"A1", 4}, {"A2", 1}, {"B1", 1}} {
		go func(id string, weight int) {
			p, err := a.AcquireNFor(ctx, id[:1], weight)
			ch <- result{id, p, err}
		}(req.id, req.weight)
		queued(t, a, i+1)
	}

	if err := a.Resize(4); err != nil {
		t.Fatal(err)
	}
	if p, err := a.TryAcquire(); p != nil || !errors.Is(err, airlock.ErrWouldBlock) {
		t.Fatalf("bypassed protected head: %v %v", p, err)
	}
	if err := a.Resize(7); err != nil {
		t.Fatal(err)
	}

	r := receive(t, ch)
	if r.id != "A1" || r.err != nil {
		t.Fatalf("protected head: %+v", r)
	}
	// Shrink before release so subsequent one-token grants remain observable in order.
	if err := a.Resize(4); err != nil {
		t.Fatal(err)
	}

	r.p.Release()

	for _, want := range []string{"A2", "B1"} {
		r = receive(t, ch)
		if r.id != want || r.err != nil {
			t.Fatalf("got %+v, want %s", r, want)
		}

		r.p.Release()
	}

	held.Release()
	assertWeightedIdle(t, a)
}

func TestResizeRejectsAllOversizedWaiters(t *testing.T) {
	a := newAirlock(t, 6, 8, 8)
	held := acquireN(t, a, "holder", 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 4)

	for i, req := range []struct {
		id     string
		weight int
	}{{"A1", 5}, {"A2", 2}, {"A3", 4}, {"B1", 4}} {
		go func(id string, weight int) {
			p, err := a.AcquireNFor(ctx, id[:1], weight)
			ch <- result{id, p, err}
		}(req.id, req.weight)
		queued(t, a, i+1)
	}

	if err := a.Resize(3); err != nil {
		t.Fatal(err)
	}

	for range 3 {
		r := receive(t, ch)
		if r.id == "A2" || r.p != nil || !errors.Is(r.err, airlock.ErrWeightExceedsCapacity) {
			t.Fatalf("rejection: %+v", r)
		}
	}

	if s := a.Stats(); s.Queued != 1 || s.WaitingTenants != 1 {
		t.Fatal(s)
	}
	if p, err := a.AcquireN(ctx, 4); p != nil || !errors.Is(err, airlock.ErrWeightExceedsCapacity) {
		t.Fatalf("new oversized request: %v %v", p, err)
	}
	if p, err := a.TryAcquireN(4); p != nil || !errors.Is(err, airlock.ErrWeightExceedsCapacity) {
		t.Fatalf("oversized try: %v %v", p, err)
	}

	held.Release()
	r := receive(t, ch)
	if r.id != "A2" || r.err != nil {
		t.Fatalf("surviving waiter: %+v", r)
	}

	r.p.Release()
	assertWeightedIdle(t, a)
}

func TestResizeCancellationGrantRace(t *testing.T) {
	for range 100 {
		a := newAirlock(t, 2, 8, 8)
		held := acquireN(t, a, "holder", 2)
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan result, 1)
		go func() {
			p, err := a.AcquireN(ctx, 2)
			ch <- result{p: p, err: err}
		}()
		queued(t, a, 1)
		var wg sync.WaitGroup

		for _, fn := range []func(){
			cancel, held.Release,
			func() {
				if err := a.Resize(1); err != nil {
					t.Error(err)
				}
			},
			func() {
				for range 10 {
					if err := a.Resize(3); err != nil {
						t.Error(err)
					}

					p, err := a.TryAcquireN(2)
					if err == nil {
						p.Release()
					}

					_ = a.Stats()
				}
			},
		} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				fn()
			}()
		}

		wg.Wait()
		r := receive(t, ch)
		if r.err == nil {
			if r.p == nil {
				t.Fatal("successful result without permit")
			}

			r.p.Release()
		} else if r.p != nil || (!errors.Is(r.err, context.Canceled) && !errors.Is(r.err, airlock.ErrWeightExceedsCapacity)) {
			t.Fatalf("race: %+v", r)
		}

		assertWeightedIdle(t, a)
	}
}

func ExampleAirlock_Resize() {
	a, err := airlock.New(airlock.Config{Capacity: 4})
	if err != nil {
		panic(err)
	}

	p, err := a.AcquireN(context.Background(), 3)
	if err != nil {
		panic(err)
	}
	defer p.Release()
	if err := a.Resize(2); err != nil {
		panic(err)
	}

	fmt.Println(a.Stats().Capacity, a.Stats().InUse)
	p.Release()
	fmt.Println(a.Stats().Capacity, a.Stats().InUse)
	// Output:
	// 2 3
	// 2 0
}

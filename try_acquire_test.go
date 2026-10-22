package airlock_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestTryAcquireEntrypointsAndQueueConfiguration(t *testing.T) {
	for _, limit := range []int{0, 4} {
		for _, tc := range []struct {
			name string
			n    int
			try  func(*airlock.Airlock) (*airlock.Permit, error)
		}{
			{"default", 1, (*airlock.Airlock).TryAcquire},
			{"named", 1, func(a *airlock.Airlock) (*airlock.Permit, error) { return a.TryAcquireFor("A") }},
			{"weighted default", 3, func(a *airlock.Airlock) (*airlock.Permit, error) { return a.TryAcquireN(3) }},
			{"weighted named", 3, func(a *airlock.Airlock) (*airlock.Permit, error) { return a.TryAcquireNFor("A", 3) }},
		} {
			t.Run(fmt.Sprintf("%s/queue=%d", tc.name, limit), func(t *testing.T) {
				a := newAirlock(t, tc.n, limit, limit)
				p, err := tc.try(a)
				if err != nil || p == nil {
					t.Fatalf("permit=%v error=%v", p, err)
				}
				t.Cleanup(p.Release)
				want := airlock.Stats{Capacity: tc.n, InUse: tc.n, ActivePermits: 1}
				if s := a.Stats(); s != want {
					t.Fatalf("got %+v, want %+v", s, want)
				}
				if q, err := tc.try(a); q != nil || !errors.Is(err, airlock.ErrWouldBlock) {
					t.Fatalf("full capacity: permit=%v error=%v", q, err)
				}
				if s := a.Stats(); s != want {
					t.Fatalf("failed Try changed state: %+v", s)
				}
				p.Release()
				p.Release()
				assertWeightedIdle(t, a)
				q, err := tc.try(a)
				if err != nil || q == nil {
					t.Fatalf("reuse: permit=%v error=%v", q, err)
				}
				q.Release()
				assertWeightedIdle(t, a)
			})
		}
	}
}

func TestTryAcquireValidationBeforeAvailability(t *testing.T) {
	a := newAirlock(t, 3, 0, 0)
	held := acquireN(t, a, "holder", 3)
	before := a.Stats()
	for _, tc := range []struct {
		name string
		try  func() (*airlock.Permit, error)
		want error
	}{
		{"empty tenant", func() (*airlock.Permit, error) { return a.TryAcquireFor("") }, airlock.ErrInvalidTenant},
		{"tenant before zero", func() (*airlock.Permit, error) { return a.TryAcquireNFor("", 0) }, airlock.ErrInvalidTenant},
		{"tenant before oversize", func() (*airlock.Permit, error) { return a.TryAcquireNFor("", 4) }, airlock.ErrInvalidTenant},
		{"default zero", func() (*airlock.Permit, error) { return a.TryAcquireN(0) }, airlock.ErrInvalidWeight},
		{"default negative", func() (*airlock.Permit, error) { return a.TryAcquireN(-1) }, airlock.ErrInvalidWeight},
		{"named zero", func() (*airlock.Permit, error) { return a.TryAcquireNFor("A", 0) }, airlock.ErrInvalidWeight},
		{"named negative", func() (*airlock.Permit, error) { return a.TryAcquireNFor("A", -1) }, airlock.ErrInvalidWeight},
		{"default oversize", func() (*airlock.Permit, error) { return a.TryAcquireN(4) }, airlock.ErrWeightExceedsCapacity},
		{"named oversize", func() (*airlock.Permit, error) { return a.TryAcquireNFor("A", 4) }, airlock.ErrWeightExceedsCapacity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if p, err := tc.try(); p != nil || !errors.Is(err, tc.want) {
				t.Fatalf("permit=%v error=%v, want %v", p, err, tc.want)
			}
			if s := a.Stats(); s != before {
				t.Fatalf("invalid request changed state: %+v", s)
			}
		})
	}
	held.Release()
	assertWeightedIdle(t, a)
}

func TestTryAcquireCannotBypassOrReorderQueuedRequests(t *testing.T) {
	a := newAirlock(t, 5, 3, 2)
	bulk := acquireN(t, a, "holder", 4)
	single := acquireN(t, a, "holder", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 3)
	for i, id := range []string{"A1", "A2", "B1"} {
		go func(id string) {
			p, err := a.AcquireNFor(ctx, id[:1], 4)
			ch <- result{id, p, err}
		}(id)
		queued(t, a, i+1)
	}
	single.Release() // Protect A1, with one free token that a small Try could use.
	before := a.Stats()
	if before.InUse != 4 || before.Queued != 3 {
		t.Fatal(before)
	}
	for _, try := range []func() (*airlock.Permit, error){
		a.TryAcquire,
		func() (*airlock.Permit, error) { return a.TryAcquireFor("A") },
		func() (*airlock.Permit, error) { return a.TryAcquireN(1) },
		func() (*airlock.Permit, error) { return a.TryAcquireNFor("new tenant", 1) },
	} {
		if p, err := try(); p != nil || !errors.Is(err, airlock.ErrWouldBlock) {
			t.Fatalf("queued request bypassed: permit=%v error=%v", p, err)
		}
		if s := a.Stats(); s != before {
			t.Fatalf("Try changed queue/accounting: before=%+v after=%+v", before, s)
		}
	}
	held := bulk
	// Each grant leaves just one free token; receive order is therefore also
	// grant order, independent of goroutine scheduling.
	for _, want := range []string{"A1", "B1", "A2"} {
		held.Release()
		r := receive(t, ch)
		if r.err != nil || r.p == nil || r.id != want {
			t.Fatalf("got %+v, want %s", r, want)
		}
		held = r.p
		t.Cleanup(held.Release)
	}
	held.Release()
	assertWeightedIdle(t, a)
}

func TestTryAcquireReleaseWakesQueuedAcquire(t *testing.T) {
	a := newAirlock(t, 3, 1, 1)
	held, err := a.TryAcquireN(3)
	if err != nil || held == nil {
		t.Fatalf("permit=%v error=%v", held, err)
	}
	t.Cleanup(held.Release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 1)
	go func() { p, err := a.AcquireN(ctx, 3); ch <- result{p: p, err: err} }()
	queued(t, a, 1)
	if p, err := a.TryAcquire(); p != nil || !errors.Is(err, airlock.ErrWouldBlock) {
		t.Fatalf("queued acquisition bypassed: permit=%v error=%v", p, err)
	}
	held.Release()
	r := receive(t, ch)
	if r.err != nil || r.p == nil {
		t.Fatal(r)
	}
	t.Cleanup(r.p.Release)
	if s := a.Stats(); s.InUse != 3 || s.ActivePermits != 1 || s.Queued != 0 || s.WaitingTenants != 0 {
		t.Fatal(s)
	}
	r.p.Release()
	assertWeightedIdle(t, a)
}

func TestTryAcquireCapacityArithmeticDoesNotOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	a := newAirlock(t, maxInt, 0, 0)
	held, err := a.TryAcquireN(maxInt - 1)
	if err != nil || held == nil {
		t.Fatalf("permit=%v error=%v", held, err)
	}
	t.Cleanup(held.Release)
	before := a.Stats()
	if p, err := a.TryAcquireNFor("A", 2); p != nil || !errors.Is(err, airlock.ErrWouldBlock) {
		t.Fatalf("overflow granted unavailable tokens: permit=%v error=%v", p, err)
	}
	if s := a.Stats(); s != before {
		t.Fatal(s)
	}
	last, err := a.TryAcquire()
	if err != nil || last == nil {
		t.Fatalf("last token: permit=%v error=%v", last, err)
	}
	if s := a.Stats(); s.InUse != maxInt || s.ActivePermits != 2 {
		t.Fatal(s)
	}
	last.Release()
	held.Release()
	all, err := a.TryAcquireN(maxInt)
	if err != nil || all == nil {
		t.Fatalf("whole capacity: permit=%v error=%v", all, err)
	}
	all.Release()
	assertWeightedIdle(t, a)
}

func TestTryAcquireMixedWithWaitingAndConcurrentRelease(t *testing.T) {
	const capacity, workers = 11, 64
	a := newAirlock(t, capacity, workers, workers)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	var active atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			weight := i%5 + 1
			tenant := fmt.Sprint(i % 4)
			var p *airlock.Permit
			var err error
			if i%2 == 0 {
				p, err = a.TryAcquireNFor(tenant, weight)
				if errors.Is(err, airlock.ErrWouldBlock) {
					if p != nil {
						t.Error("failed Try returned a permit")
						p.Release()
						return
					}
					p, err = a.AcquireNFor(ctx, tenant, weight)
				}
			} else {
				p, err = a.AcquireNFor(ctx, tenant, weight)
			}
			if err != nil || p == nil {
				t.Errorf("permit=%v error=%v", p, err)
				return
			}
			if n := active.Add(int64(weight)); n > capacity {
				t.Errorf("concurrent work holds %d tokens, capacity=%d", n, capacity)
			}
			if s := a.Stats(); s.InUse < weight || s.InUse > capacity || s.ActivePermits < 1 || s.ActivePermits > s.InUse {
				t.Error(s)
			}
			active.Add(-int64(weight))
			var releases sync.WaitGroup
			releases.Add(2)
			for j := 0; j < 2; j++ {
				go func() { defer releases.Done(); p.Release() }()
			}
			releases.Wait()
		}(i)
	}
	close(start)
	wg.Wait()
	if n := active.Load(); n != 0 {
		t.Fatalf("work accounting leaked %d tokens", n)
	}
	assertWeightedIdle(t, a)
}

var _ interface {
	TryAcquire() (*airlock.Permit, error)
	TryAcquireFor(string) (*airlock.Permit, error)
	TryAcquireN(int) (*airlock.Permit, error)
	TryAcquireNFor(string, int) (*airlock.Permit, error)
} = (*airlock.Airlock)(nil)

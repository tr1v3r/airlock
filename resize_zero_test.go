package airlock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestResizeZeroPauseResume(t *testing.T) {
	a := newAirlock(t, 4, 4, 2)
	held := acquireN(t, a, "holder", 3)
	defer held.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	head := make(chan result, 1)
	go func() { p, e := a.AcquireNFor(ctx, "head", 3); head <- result{p: p, err: e} }()
	queued(t, a, 1) // Select and protect the heavy head while one token is free.
	if err := a.Resize(0); err != nil {
		t.Fatal(err)
	}
	if err := a.Resize(0); err != nil {
		t.Fatal(err)
	}
	if s := a.Stats(); s.Capacity != 0 || s.InUse != 3 || s.Queued != 1 {
		t.Fatal(s)
	}
	for _, weight := range []int{1, 100} {
		if p, e := a.TryAcquireN(weight); p != nil || !errors.Is(e, airlock.ErrWouldBlock) {
			t.Fatal(p, e)
		}
	}
	if _, e := a.TryAcquireN(0); !errors.Is(e, airlock.ErrInvalidWeight) {
		t.Fatal(e)
	}
	tail := make(chan result, 1)
	go func() { p, e := a.AcquireNFor(ctx, "tail", 1); tail <- result{p: p, err: e} }()
	queued(t, a, 2)
	oversized := make(chan result, 1)
	go func() { p, e := a.AcquireNFor(ctx, "large", 100); oversized <- result{p: p, err: e} }()
	queued(t, a, 3)
	if err := a.Resize(4); err != nil {
		t.Fatal(err)
	}
	if r := receive(t, oversized); !errors.Is(r.err, airlock.ErrWeightExceedsCapacity) {
		t.Fatal(r)
	}
	select {
	case r := <-tail:
		t.Fatalf("bypassed protected head: %+v", r)
	default:
	}
	held.Release()
	for _, ch := range []chan result{head, tail} {
		r := receive(t, ch)
		if r.err != nil {
			t.Fatal(r.err)
		}
		r.p.Release()
	}
	assertWeightedIdle(t, a)
}

func TestResizeZeroPriorityTurns(t *testing.T) {
	cfg := priorityConfig(3)
	cfg.Priority.Groups[0].Turns = 2
	cfg.Priority.Groups[1].Turns = 1
	a := newPriority(t, cfg)
	held := acquireN(t, a, "holder", 3)
	if err := a.Resize(0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 5)
	// Low arrives first but must not become protected during the pause.
	for i, tenant := range []string{"L", "A", "A", "B"} {
		priorityEnqueue(ctx, t, a, ch, []string{"L0", "A0", "A1", "B0"}[i], tenant, 3, i+1)
	}
	other, cancelOther := context.WithCancel(ctx)
	defer cancelOther()
	priorityEnqueue(other, t, a, ch, "M0", "M", 3, 5)
	cancelOther()
	if r := receive(t, ch); r.id != "M0" || r.p != nil || !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}
	held.Release() // Cancellation and release must not select a head while paused.
	if s := a.Stats(); s.Capacity != 0 || s.InUse != 0 || s.Queued != 4 {
		t.Fatal(s)
	}
	if err := a.Resize(3); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"A0", "B0", "L0", "A1"} {
		held = priorityNext(t, held, ch, want)
	}
	held.Release()
	assertWeightedIdle(t, a)
}

func TestResizeZeroPriorityProtectedHead(t *testing.T) {
	for _, capacity := range []int{5, 3} {
		t.Run(map[int]string{5: "preserved", 3: "rejected"}[capacity], func(t *testing.T) {
			cfg := priorityConfig(5)
			cfg.Priority.Groups[0].Turns = 2
			cfg.Priority.Groups[1].Turns = 1
			a := newPriority(t, cfg)
			held := acquireN(t, a, "holder", 4)
			single := acquireN(t, a, "holder", 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch := make(chan result, 4)
			priorityEnqueue(ctx, t, a, ch, "A0", "A", 4, 1)
			for i, tenant := range []string{"A", "B", "L"} {
				priorityEnqueue(ctx, t, a, ch, []string{"A1", "B0", "L0"}[i], tenant, 3, i+2)
			}
			single.Release() // Protect A0, consume the first high turn, rotate A behind B.
			if err := a.Resize(0); err != nil {
				t.Fatal(err)
			}
			if err := a.Resize(capacity); err != nil {
				t.Fatal(err)
			}
			if capacity == 3 {
				if r := receive(t, ch); r.id != "A0" || r.p != nil || !errors.Is(r.err, airlock.ErrWeightExceedsCapacity) {
					t.Fatal(r)
				}
			}
			select {
			case r := <-ch:
				t.Fatalf("grant before holder released: %+v", r)
			default:
			}
			if capacity == 5 {
				held = priorityNext(t, held, ch, "A0")
			}
			// Preserving or rejecting the selected head must not spend another turn
			// or reset the tenant rotation: B gets the second high slot, then low.
			for _, want := range []string{"B0", "L0", "A1"} {
				held = priorityNext(t, held, ch, want)
			}
			held.Release()
			assertWeightedIdle(t, a)
		})
	}
}

func TestResizeZeroReleaseCancelRace(t *testing.T) {
	for range 32 {
		a := newPriority(t, priorityConfig(2))
		held := acquireN(t, a, "holder", 2)
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan result, 1)
		priorityEnqueue(ctx, t, a, ch, "A", "A", 2, 1)
		if err := a.Resize(0); err != nil {
			cancel()
			t.Fatal(err)
		}
		start := make(chan struct{})
		done := make(chan error, 3)
		for _, fn := range []func() error{
			func() error { held.Release(); held.Release(); return nil },
			func() error { cancel(); return nil },
			func() error {
				for _, capacity := range []int{1, 0, 2, 0, 1, 2} {
					if err := a.Resize(capacity); err != nil {
						return err
					}
				}
				return nil
			},
		} {
			go func() { <-start; done <- fn() }()
		}
		close(start)
		timer := time.NewTimer(3 * time.Second)
		for range 3 {
			select {
			case err := <-done:
				if err != nil {
					timer.Stop()
					t.Fatal(err)
				}
			case <-timer.C:
				t.Fatal("release/cancel/resize race did not finish")
			}
		}
		timer.Stop()
		r := receive(t, ch)
		if r.err == nil {
			if r.p == nil {
				t.Fatal("successful acquisition without permit")
			}
			r.p.Release()
		} else if r.p != nil || (!errors.Is(r.err, context.Canceled) && !errors.Is(r.err, airlock.ErrWeightExceedsCapacity)) {
			t.Fatalf("unexpected race outcome: %+v", r)
		}
		assertWeightedIdle(t, a)
		// The raced waiter must leave neither a stale protected head nor lost capacity.
		p, err := a.TryAcquireNFor("B", 2)
		if err != nil {
			t.Fatal(err)
		}
		p.Release()
		assertWeightedIdle(t, a)
	}
}

func TestResizeZeroBudgetsAndCancellation(t *testing.T) {
	a := newAirlock(t, 1, 1, 1)
	if err := a.Resize(0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 1)
	go func() { p, e := a.AcquireN(ctx, 100); ch <- result{p: p, err: e} }()
	queued(t, a, 1)
	if _, err := a.Acquire(context.Background()); !errors.Is(err, airlock.ErrQueueFull) {
		t.Fatal(err)
	}
	cancel()
	if r := receive(t, ch); !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}
	if _, err := a.AcquireN(ctx, 100); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := a.Resize(-1); !errors.Is(err, airlock.ErrInvalidConfig) {
		t.Fatal(err)
	}
	if s := a.Stats(); s.Capacity != 0 || s.Queued != 0 {
		t.Fatal(s)
	}
	if _, err := airlock.New(airlock.Config{Capacity: 0}); !errors.Is(err, airlock.ErrInvalidConfig) {
		t.Fatal(err)
	}
}

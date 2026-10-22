package fairgate_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/airlock/internal/fairgate"
)

func newGate(t *testing.T, capacity, total, tenant int) *fairgate.Gate {
	t.Helper()
	g, err := fairgate.New(fairgate.Config{Capacity: capacity, MaxQueued: total, MaxQueuedPerTenant: tenant})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func acquire(t *testing.T, g *fairgate.Gate, tenant string) *fairgate.Permit {
	t.Helper()
	p, err := g.Acquire(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Release)
	return p
}

func queued(t *testing.T, g *fairgate.Gate, n int) {
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
	p   *fairgate.Permit
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
	g := newGate(t, 1, 20, 10)
	held := acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 10)
	for i, id := range []string{"A1", "A2", "A3", "B1", "B2", "C1"} {
		go func(id string) { p, err := g.Acquire(ctx, id[:1]); ch <- result{id, p, err} }(id)
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
	g := newGate(t, 1, 3, 2)
	held := acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 3)
	for i, tenant := range []string{"A", "A", "B"} {
		go func(tenant string) { p, err := g.Acquire(ctx, tenant); ch <- result{tenant, p, err} }(tenant)
		queued(t, g, i+1)
		if i == 1 {
			if _, err := g.Acquire(ctx, "A"); !errors.Is(err, fairgate.ErrTenantQueueFull) {
				t.Fatal(err)
			}
		}
	}
	if _, err := g.Acquire(ctx, "A"); !errors.Is(err, fairgate.ErrQueueFull) {
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
	for _, cfg := range []fairgate.Config{{}, {Capacity: -1}, {Capacity: 1, MaxQueued: -1}, {Capacity: 1, MaxQueuedPerTenant: -1}, {Capacity: 1, MaxQueued: 1, MaxQueuedPerTenant: 2}} {
		if _, err := fairgate.New(cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	g := newGate(t, 1, 0, 0)
	if _, err := g.Acquire(context.Background(), ""); !errors.Is(err, fairgate.ErrInvalidTenant) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Acquire(ctx, "A"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p := acquire(t, g, "A")
	if _, err := g.Acquire(context.Background(), "B"); !errors.Is(err, fairgate.ErrQueueFull) {
		t.Fatal(err)
	}
	p.Release()
	defer func() {
		if recover() == nil {
			t.Error("nil context did not panic")
		}
	}()
	g.Acquire(nil, "A")
}

func TestCancelGrantRaceAndDuplicateRelease(t *testing.T) {
	for i := 0; i < 200; i++ {
		g := newGate(t, 1, 1, 1)
		held := acquire(t, g, "holder")
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan result, 1)
		go func() { p, err := g.Acquire(ctx, "A"); ch <- result{p: p, err: err} }()
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
	g := newGate(t, 8, 512, 512)
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p, err := g.Acquire(context.Background(), fmt.Sprint(i))
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
	g := newGate(t, 8, 0, 0)
	for i := 0; i < 8; i++ {
		acquire(t, g, "A")
	}
	if s := g.Stats(); s.InUse != 8 {
		t.Fatal(s)
	}
}

func TestGrantedSlotCannotBeStolen(t *testing.T) {
	g := newGate(t, 1, 1, 1)
	held := acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan result, 1)
	go func() { p, err := g.Acquire(ctx, "A"); ch <- result{p: p, err: err} }()
	queued(t, g, 1)
	held.Release() // reserves the slot for A before waking its goroutine
	cancel()       // cancellation cannot revoke that reservation
	r := receive(t, ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(r.p.Release)
	ctxB, cancelB := context.WithCancel(context.Background())
	go func() { p, err := g.Acquire(ctxB, "B"); ch <- result{p: p, err: err} }()
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

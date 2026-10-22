package airlock_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestAirlockErrorPrecedence(t *testing.T) {
	g := newAirlock(t, 1, 1, 1)
	acquire(t, g, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := make(chan result, 1)
	go func() { p, err := g.Acquire(ctx, "A"); ch <- result{p: p, err: err} }()
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
		p, err := g.Acquire(tc.ctx, tc.tenant)
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
	if _, err := g.Acquire(context.Background(), "B"); !errors.Is(err, airlock.ErrTenantQueueFull) {
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
			p, err := g.Acquire(requestCtx, id[:1])
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
	go func() { p, err := g.Acquire(context.Background(), "A"); ch <- result{"A3", p, err} }()
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
			p, err := g.Acquire(requestCtx, id[:1])
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
			go func() { p, err := g.Acquire(ctx, id); ch <- result{id, p, err} }()
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
	if p, err := g.Acquire(ctx, "B"); p != nil || !errors.Is(err, context.DeadlineExceeded) {
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
	p, err := g.Acquire(context.Background(), "tenant-a")
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

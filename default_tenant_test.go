package airlock_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/tr1v3r/airlock"
)

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

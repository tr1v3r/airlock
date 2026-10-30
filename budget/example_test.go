package budget_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/budget"
)

func ExampleNew() {
	// examplePool below is an in-process fixture. Applications supply their
	// shared Redis or PostgreSQL driver; stage-one leases do not stop external work.
	manager, err := budget.New(budget.Options{
		Driver:    &examplePool{},
		Resource:  "workers",
		GatewayID: "gateway-a",
		Policy: airlock.ResourceConfig{
			Admission: airlock.Config{Capacity: 8},
			LeaseTTL:  time.Minute,
		},
		Denominations: []int{4, 1},
		MaxHeld:       4,
	})
	if err != nil {
		panic(err)
	}
	closeManager := func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := manager.Close(cleanup); err != nil {
			panic(err)
		}
	}
	defer closeManager()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Acquire waits for the control loop to obtain enough budget for all four
	// tokens. No caller-side sleep or readiness polling is needed.
	permit, err := manager.Acquire(ctx, "online", 4)
	if err != nil {
		panic(err)
	}
	defer permit.Release()
	fmt.Println("in use:", manager.Stats().InUse)

	extra, err := manager.TryAcquire("batch", 1)
	if extra != nil {
		extra.Release()
	}
	fmt.Println("full:", errors.Is(err, airlock.ErrWouldBlock))

	// Finish the actual work before releasing its permit. Stop/join workers
	// before Close; Close waits for outstanding permits, it does not cancel work.
	permit.Release()
	closeManager()
	_, err = manager.TryAcquire("online", 1)
	fmt.Println("closed:", errors.Is(err, budget.ErrClosed))
	// Output:
	// in use: 4
	// full: true
	// closed: true
}

// examplePool is a single-resource, in-memory Driver fixture for the executable
// example. Each operation runs a structured transition under one mutex. It is
// not a durable backend or a replacement for the independent driver modules.
type examplePool struct {
	mu    sync.Mutex
	state *airlock.SharedState
}

// apply atomically publishes the transition, including state accompanying a
// business error. Backend time is sampled inside the serialization boundary.
func (p *examplePool) apply(ctx context.Context, cmd airlock.Command) (airlock.Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return airlock.Result{}, err
	}
	next, result, _, err := airlock.Transition(p.state, time.Now().UTC(), cmd)
	if next != nil {
		p.state = next
	}
	return result, err
}

// Submit acquires or queues a whole chunk.
func (p *examplePool) Submit(ctx context.Context, r airlock.Request) (airlock.Result, error) {
	return p.apply(ctx, airlock.Command{Kind: airlock.Submit, Request: r})
}

// Observe looks up a request and applies due maintenance.
func (p *examplePool) Observe(ctx context.Context, r airlock.Request) (airlock.Result, error) {
	return p.apply(ctx, airlock.Command{Kind: airlock.Observe, Request: r})
}

// Cancel terminates a queued or granted request.
func (p *examplePool) Cancel(ctx context.Context, r airlock.Request) (airlock.Result, error) {
	return p.apply(ctx, airlock.Command{Kind: airlock.Cancel, Request: r})
}

// Renew extends an owned chunk lease.
func (p *examplePool) Renew(ctx context.Context, r airlock.LeaseRef) (airlock.Result, error) {
	return p.apply(ctx, airlock.Command{Kind: airlock.Renew, Request: r.Request, LeaseID: r.ID})
}

// Release returns an owned chunk lease.
func (p *examplePool) Release(ctx context.Context, r airlock.LeaseRef) (airlock.Result, error) {
	return p.apply(ctx, airlock.Command{Kind: airlock.Release, Request: r.Request, LeaseID: r.ID})
}

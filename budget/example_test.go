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
	// exampleDriver below is an in-process fixture. Applications supply their
	// shared Redis or PostgreSQL driver; stage-one leases do not stop external work.
	manager, err := budget.New(budget.Options{
		Driver:    &exampleDriver{},
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
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelClose()
		if err := manager.Close(closeCtx); err != nil {
			panic(err)
		}
	}
	defer closeManager()

	acquireCtx, cancelAcquire := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAcquire()
	// Acquire waits for the control loop to obtain enough budget for all four
	// tokens. No caller-side sleep or readiness polling is needed.
	permit, err := manager.Acquire(acquireCtx, "online", 4)
	if err != nil {
		panic(err)
	}
	defer permit.Release()
	fmt.Println("in use:", manager.Stats().InUse)

	batchPermit, err := manager.TryAcquire("batch", 1)
	if batchPermit != nil {
		batchPermit.Release()
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

// exampleDriver is a single-resource, in-memory Driver fixture for the executable
// example. Each operation runs a structured transition under one mutex. It is
// not a durable backend or a replacement for the independent driver modules.
type exampleDriver struct {
	mu    sync.Mutex
	state *airlock.SharedState
}

// apply atomically publishes the transition, including state accompanying a
// business error. Backend time is sampled inside the serialization boundary.
func (d *exampleDriver) apply(ctx context.Context, command airlock.Command) (airlock.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return airlock.Result{}, err
	}
	nextState, result, _, err := airlock.Transition(d.state, time.Now().UTC(), command)
	if nextState != nil {
		d.state = nextState
	}
	return result, err
}

// Submit acquires or queues a whole chunk.
func (d *exampleDriver) Submit(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.apply(ctx, airlock.Command{Kind: airlock.Submit, Request: request})
}

// Observe looks up a request and applies due maintenance.
func (d *exampleDriver) Observe(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.apply(ctx, airlock.Command{Kind: airlock.Observe, Request: request})
}

// Cancel terminates a queued or granted request.
func (d *exampleDriver) Cancel(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.apply(ctx, airlock.Command{Kind: airlock.Cancel, Request: request})
}

// Renew extends an owned chunk lease.
func (d *exampleDriver) Renew(ctx context.Context, leaseRef airlock.LeaseRef) (airlock.Result, error) {
	return d.apply(ctx, airlock.Command{Kind: airlock.Renew, Request: leaseRef.Request, LeaseID: leaseRef.ID})
}

// Release returns an owned chunk lease.
func (d *exampleDriver) Release(ctx context.Context, leaseRef airlock.LeaseRef) (airlock.Result, error) {
	return d.apply(ctx, airlock.Command{Kind: airlock.Release, Request: leaseRef.Request, LeaseID: leaseRef.ID})
}

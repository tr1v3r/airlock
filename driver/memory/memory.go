// Package memory provides an in-process reference for Airlock's durable protocol.
// Use it for Shared API tests and demos; use airlock.New for ordinary local admission.
// It simulates leases and recovery, not network partitions or durable storage.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/tr1v3r/airlock"
)

type resource struct {
	gate     chan struct{}
	snapshot []byte
}

// Driver owns volatile resource snapshots. Independent instances do not share
// capacity. Resource configurations remain allocated for the driver's lifetime;
// terminal request records are reclaimed by subsequent operations. Do not copy.
type Driver struct {
	mu        sync.Mutex
	resources map[string]*resource
}

// New creates a reference driver. The zero value is also usable.
func New() *Driver { return &Driver{} }

// execute serializes each resource and uses the shared durable state machine.
func (d *Driver) execute(ctx context.Context, cmd airlock.Command) (airlock.Result, error) {
	if err := ctx.Err(); err != nil {
		return airlock.Result{}, err
	}
	if err := airlock.ValidateCommand(cmd); err != nil {
		return airlock.Result{}, err
	}
	d.mu.Lock()
	if d.resources == nil {
		d.resources = make(map[string]*resource)
	}
	r := d.resources[cmd.Request.Resource]
	if r == nil {
		r = &resource{gate: make(chan struct{}, 1)}
		d.resources[cmd.Request.Resource] = r
	}
	d.mu.Unlock()
	select {
	case r.gate <- struct{}{}:
		defer func() { <-r.gate }()
	case <-ctx.Done():
		return airlock.Result{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return airlock.Result{}, err
	}
	next, result, err := airlock.Transition(r.snapshot, time.Now(), cmd)
	if next != nil {
		r.snapshot = next
	}
	if err := ctx.Err(); err != nil {
		return airlock.Result{}, err
	}
	return result, err
}

var _ airlock.Driver = (*Driver)(nil)

// Submit atomically submits the whole request without refreshing an existing lease.
func (d *Driver) Submit(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Submit, Request: request})
}

// Observe returns the request outcome after advancing expiry and scheduling.
func (d *Driver) Observe(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Observe, Request: request})
}

// Cancel cancels the request, including a grant or a cancellation before submission.
func (d *Driver) Cancel(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Cancel, Request: request})
}

// Renew extends a live lease matching the original request and lease ID.
func (d *Driver) Renew(ctx context.Context, lease airlock.LeaseRef) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Renew, Request: lease.Request, LeaseID: lease.ID})
}

// Release ends a matching lease and returns its whole weight at most once.
func (d *Driver) Release(ctx context.Context, lease airlock.LeaseRef) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Release, Request: lease.Request, LeaseID: lease.ID})
}

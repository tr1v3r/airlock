// Package memory provides an in-process reference for Airlock's durable protocol.
// Use it for Shared API tests and demos; use airlock.New for ordinary local admission.
// It simulates leases and recovery, not network partitions or durable storage.
// All operations follow airlock.Driver: reuse the original request, disregard
// results on error, and remember that cancellation can follow a committed change.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/tr1v3r/airlock/protocol"
)

// resource holds one volatile protocol snapshot and its serialization gate.
type resource struct {
	// gate serializes transitions for this resource with context-aware waiting.
	gate chan struct{}
	// snapshot is the last committed opaque state; gate protects access.
	snapshot []byte
}

// Driver owns volatile resource snapshots. Independent instances do not share
// capacity. Resource configurations remain allocated for the driver's lifetime;
// terminal request records are reclaimed by subsequent operations. It is safe
// for concurrent use. Do not copy. Methods require non-nil contexts.
type Driver struct {
	// mu protects resource lookup and lazy allocation, not individual transitions.
	mu sync.Mutex
	// resources retains independent per-resource gates and snapshots for this driver's lifetime.
	resources map[string]*resource
}

// New creates a reference driver. The zero value is also usable.
func New() *Driver { return &Driver{} }

// execute serializes each resource and uses the shared durable state machine.
func (d *Driver) execute(ctx context.Context, cmd protocol.Command) (protocol.Result, error) {
	if err := ctx.Err(); err != nil {
		return protocol.Result{}, err
	}
	if err := protocol.ValidateCommand(cmd); err != nil {
		return protocol.Result{}, err
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
		return protocol.Result{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return protocol.Result{}, err
	}
	state, err := protocol.DecodeState(r.snapshot)
	if err != nil {
		return protocol.Result{}, err
	}
	next, result, _, err := protocol.Transition(state, time.Now(), cmd)
	if next != nil {
		data, codecErr := protocol.EncodeState(next)
		if codecErr != nil {
			return protocol.Result{}, codecErr
		}
		r.snapshot = data
	}
	if err := ctx.Err(); err != nil {
		return protocol.Result{}, err
	}
	return result, err
}

// Verify the reference backend implements the public Driver contract.
var _ protocol.Driver = (*Driver)(nil)

// Submit atomically submits the whole request without refreshing an existing lease.
func (d *Driver) Submit(ctx context.Context, request protocol.Request) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Submit, Request: request})
}

// Observe returns the request outcome after advancing expiry and scheduling.
func (d *Driver) Observe(ctx context.Context, request protocol.Request) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Observe, Request: request})
}

// Cancel cancels the request, including a grant or a cancellation before submission.
func (d *Driver) Cancel(ctx context.Context, request protocol.Request) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Cancel, Request: request})
}

// Renew extends a live lease matching the original request and lease ID.
func (d *Driver) Renew(ctx context.Context, lease protocol.LeaseRef) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Renew, Request: lease.Request, LeaseID: lease.ID})
}

// Release ends a matching lease and returns its whole weight at most once.
func (d *Driver) Release(ctx context.Context, lease protocol.LeaseRef) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Release, Request: lease.Request, LeaseID: lease.ID})
}

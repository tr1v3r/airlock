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

	"github.com/tr1v3r/airlock"
)

// resource holds one volatile protocol snapshot and its serialization gate.
type resource struct {
	// gate serializes transitions for this resource with context-aware waiting.
	gate chan struct{}
	// snapshot is the last committed opaque state; gate protects access.
	snapshot []byte
	// state and observation are immutable projections of snapshot, protected by gate.
	state       *airlock.SharedState
	observation airlock.Observation
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
	if cmd.Kind == airlock.Observe && r.state != nil {
		o := r.observation
		o.Record = r.state.Records[cmd.Request.ID]
		if result, ready, err := o.Observe(time.Now(), cmd.Request); ready || err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return airlock.Result{}, ctxErr
			}
			return result, err
		}
	}
	state, err := airlock.DecodeState(r.snapshot)
	if err != nil {
		return airlock.Result{}, err
	}
	next, result, _, err := airlock.Transition(state, time.Now(), cmd)
	if next != nil {
		data, codecErr := airlock.EncodeState(next)
		if codecErr != nil {
			return airlock.Result{}, codecErr
		}
		r.snapshot = data
		r.state = next
		r.observation = airlock.Observation{Resource: next.Resource, Config: next.Config, Revision: next.Revision,
			Time: next.Time, NextMaintenance: next.NextMaintenance()}
	}
	if err := ctx.Err(); err != nil {
		return airlock.Result{}, err
	}
	return result, err
}

// Verify the reference backend implements the public Driver contract.
var _ airlock.Driver = (*Driver)(nil)

// Submit atomically submits the whole request without refreshing an existing lease.
func (d *Driver) Submit(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return d.execute(ctx, airlock.Command{Kind: airlock.Submit, Request: request})
}

// Observe reads the committed outcome, advancing expiry and scheduling when due.
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

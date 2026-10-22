// Package memory provides an in-process reference for Airlock's durable protocol.
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

// Execute serializes each resource and uses the shared durable state machine.
func (d *Driver) Execute(ctx context.Context, cmd airlock.Command) (airlock.Result, error) {
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

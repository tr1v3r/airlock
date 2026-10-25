// Package airlock provides weighted tenant admission locally and through shared drivers.
package airlock

import (
	"context"

	"github.com/tr1v3r/airlock/internal/scheduler"
)

// Config defines fixed limits for an Airlock. Queue limits count pending
// requests, not granted permits. Zero queue limits disable waiting.
type Config = scheduler.Config

// Stats returns a consistent snapshot, not a completion barrier.
type Stats = scheduler.Stats

// Airlock controls weighted admission with FIFO waiting within each tenant
// and round-robin selection across nonempty tenant queues within each priority
// group. Optional priority groups receive weighted selection opportunities.
// New waiting tenants join the end of their group's rotation.
// It does not start workers or track completion.
//
// A selected request waits for all its tokens before any later request can be
// granted. This prevents starvation of large requests, but can leave capacity
// idle. Progress requires holders to eventually release their permits.
// Fairness applies to request selection, not token counts, execution time or
// requests rejected before queueing. Running work is never preempted. One tenant
// may hold every token when others are idle. Use New; the zero value is not usable.
// An Airlock is safe for concurrent use and must not be copied.
type Airlock struct{ scheduler *scheduler.Airlock }

// Permit must not be copied. Release is idempotent, including concurrent calls.
// Cancellation after a successful Acquire does not release it automatically.
// Obtain permits from an Acquire or TryAcquire method; a zero-value Permit
// holds no capacity.
type Permit = scheduler.Permit

// PriorityGroup reserves waiting space and selection opportunities for a group
// of tenants. It does not reserve execution capacity or preempt running work.
type PriorityGroup = scheduler.PriorityGroup

// PriorityPolicy maps trusted tenant IDs to fixed priority groups. New snapshots
// the configuration; later changes to these maps or slices have no effect.
// Callers must not mutate the policy concurrently with New.
type PriorityPolicy = scheduler.PriorityPolicy

// ErrInvalidConfig reports invalid limits or a priority policy.
var ErrInvalidConfig = scheduler.ErrInvalidConfig

// ErrInvalidTenant reports an empty explicitly supplied tenant ID.
var ErrInvalidTenant = scheduler.ErrInvalidTenant

// ErrInvalidWeight reports a nonpositive requested token count.
var ErrInvalidWeight = scheduler.ErrInvalidWeight

// ErrWeightExceedsCapacity reports a request larger than the fixed capacity.
var ErrWeightExceedsCapacity = scheduler.ErrWeightExceedsCapacity

// ErrQueueFull reports that the global waiting queue is full.
var ErrQueueFull = scheduler.ErrQueueFull

// ErrGroupQueueFull reports that the tenant's priority group waiting queue is full.
var ErrGroupQueueFull = scheduler.ErrGroupQueueFull

// ErrTenantQueueFull reports that the tenant's waiting queue is full.
var ErrTenantQueueFull = scheduler.ErrTenantQueueFull

// ErrWouldBlock reports that TryAcquire cannot grant capacity immediately
// without bypassing a queued request. The attempt does not join the queue.
var ErrWouldBlock = scheduler.ErrWouldBlock

// New constructs an Airlock, or returns ErrInvalidConfig for invalid limits or
// priority policy. It snapshots the policy. It has no unlimited mode; limits,
// tenant assignments and selection shares cannot be changed after creation.
func New(c Config) (*Airlock, error) {
	s, err := scheduler.New(c)
	if err != nil {
		return nil, err
	}
	return &Airlock{scheduler: s}, nil
}

// Stats returns a consistent snapshot, not a completion barrier.
func (a *Airlock) Stats() Stats { return a.scheduler.Stats() }

// Acquire atomically acquires one slot for a shared default tenant, distinct
// from every named tenant. It obeys the same queue limits and scheduling rules
// as AcquireFor. A nil context panics.
//
// An already-cancelled context returns ctx.Err(). If capacity cannot be granted,
// global, priority-group and then per-tenant queue limits are checked, in that
// order. Queue rejection does not wait for ctx. Immediate grants do not consume
// queue space or scheduling turns.
//
// The first state transition under the lock wins cancellation/grant races.
// A grant that wins may return success even after ctx is cancelled; the caller
// must release every successful permit. An error retains no token.
// Cancellation does not stop admitted work.
func (a *Airlock) Acquire(ctx context.Context) (*Permit, error) {
	return a.scheduler.Acquire(ctx)
}

// AcquireFor acquires one slot for tenantID, using the same cancellation,
// queue-limit and release rules as Acquire. A nil context panics. An empty ID
// returns ErrInvalidTenant before cancellation or capacity is checked.
// IDs are exact strings supplied by the caller. Use trusted, bounded IDs:
// this package neither authenticates tenants nor limits their string length.
func (a *Airlock) AcquireFor(ctx context.Context, tenant string) (*Permit, error) {
	return a.scheduler.AcquireFor(ctx, tenant)
}

// AcquireN atomically acquires n tokens for the default tenant. A nil context
// panics. Nonpositive n returns ErrInvalidWeight; n greater than Capacity returns
// ErrWeightExceedsCapacity. Weight validation precedes cancellation checks.
// It never holds a partial allocation. The queue and cancellation rules of
// Acquire apply, and the returned Permit releases all n tokens at once.
func (a *Airlock) AcquireN(ctx context.Context, n int) (*Permit, error) {
	return a.scheduler.AcquireN(ctx, n)
}

// AcquireNFor atomically acquires n tokens for tenantID. Validation checks nil
// context, then tenant ID, then weight, before cancellation or capacity.
// Tenant identity rules are the same as AcquireFor; all other rules are those
// of AcquireN.
func (a *Airlock) AcquireNFor(ctx context.Context, tenant string, n int) (*Permit, error) {
	return a.scheduler.AcquireNFor(ctx, tenant, n)
}

// TryAcquire atomically acquires one token for the shared default tenant without
// joining a queue or waiting for capacity. If capacity is insufficient or any
// request is queued, it returns a nil permit and ErrWouldBlock without changing
// admission state. It never bypasses waiting requests, even when tokens are idle.
//
// Queue limits do not affect TryAcquire. The method has no context because it
// does not wait for capacity, but it still acquires the internal mutex and is
// not wait-free. A successful permit must be released, as with Acquire.
func (a *Airlock) TryAcquire() (*Permit, error) {
	return a.scheduler.TryAcquire()
}

// TryAcquireFor attempts to acquire one token for tenantID, with the same rules
// as TryAcquire. An empty ID returns ErrInvalidTenant before checking capacity.
// Tenant IDs follow the same trust and identity rules as AcquireFor.
func (a *Airlock) TryAcquireFor(tenant string) (*Permit, error) {
	return a.scheduler.TryAcquireFor(tenant)
}

// TryAcquireN attempts to acquire all n tokens for the default tenant, with the
// same rules as TryAcquire. Nonpositive n returns ErrInvalidWeight; n greater
// than Capacity returns ErrWeightExceedsCapacity. Validation precedes checking
// availability. Failure never holds a partial allocation. The returned permit
// releases all n tokens together.
func (a *Airlock) TryAcquireN(n int) (*Permit, error) {
	return a.scheduler.TryAcquireN(n)
}

// TryAcquireNFor attempts to acquire all n tokens for tenantID. It validates the
// nonempty tenant ID before weight or capacity, then follows TryAcquireN. Like
// TryAcquireFor, it neither queues nor changes a tenant's rotation position.
func (a *Airlock) TryAcquireNFor(tenant string, n int) (*Permit, error) {
	return a.scheduler.TryAcquireNFor(tenant, n)
}

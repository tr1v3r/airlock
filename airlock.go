package airlock

import (
	"context"
	"errors"
	"sync"

	"github.com/tr1v3r/airlock/internal/scheduler"
	"github.com/tr1v3r/airlock/protocol"
)

// Admission and scheduling types are shared with the protocol package's
// reference state machine through the internal scheduler core; these aliases
// keep one identity for application configuration.
type (
	// Config defines fixed limits for an Airlock. Queue limits count pending
	// requests, not granted permits. Zero queue limits disable waiting.
	Config = scheduler.Config
	// Stats is a consistent snapshot, not a completion barrier. Capacity and
	// InUse count tokens; ActivePermits counts granted, unreleased permits,
	// and Queued counts pending requests, including a protected head request.
	Stats = scheduler.Stats
)

// Admission errors are shared between local admission, the scheduling core and
// the shared protocol; each alias preserves one identity across packages.
var (
	// ErrInvalidConfig reports invalid limits or a priority policy.
	ErrInvalidConfig = scheduler.ErrInvalidConfig
	// ErrInvalidWeight reports a nonpositive requested token count.
	ErrInvalidWeight = protocol.ErrInvalidWeight
	// ErrWeightExceedsCapacity reports a request larger than the fixed capacity.
	ErrWeightExceedsCapacity = protocol.ErrWeightExceedsCapacity
	// ErrQueueFull reports that the global waiting queue is full.
	ErrQueueFull = scheduler.ErrQueueFull
	// ErrGroupQueueFull reports that the tenant's priority group waiting queue is full.
	ErrGroupQueueFull = scheduler.ErrGroupQueueFull
	// ErrTenantQueueFull reports that the tenant's waiting queue is full.
	ErrTenantQueueFull = scheduler.ErrTenantQueueFull
	// ErrInvalidTenant reports an empty explicitly supplied tenant ID.
	ErrInvalidTenant = errors.New("airlock: empty tenant")
	// ErrWouldBlock reports that TryAcquire cannot grant capacity immediately
	// without bypassing a queued request. The attempt does not join the queue.
	ErrWouldBlock = errors.New("airlock: acquisition would block")
)

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
type Airlock struct {
	// mu serializes every scheduling transition and waiter handoff.
	mu sync.Mutex
	// sched is the single-threaded scheduling core owned by this Airlock.
	sched *scheduler.Scheduler
}

// Permit must not be copied. Release is idempotent, including concurrent calls.
// Cancellation after a successful Acquire does not release it automatically.
// Obtain permits from an Acquire or TryAcquire method; a zero-value Permit
// holds no capacity.
type Permit struct {
	// once returns capacity at most once, even under concurrent Release calls.
	once sync.Once
	// owner receives released capacity; it is cleared after release.
	owner *Airlock
	// weight is the complete token allocation returned by Release.
	weight int
}

// New constructs an Airlock, or returns ErrInvalidConfig for invalid limits or
// priority policy. It snapshots the policy. It has no unlimited mode. Resize
// changes capacity; queue limits, tenant assignments and selection shares remain fixed.
func New(cfg Config) (*Airlock, error) {
	sched, err := scheduler.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Airlock{sched: sched}, nil
}

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
func (g *Airlock) Acquire(ctx context.Context) (*Permit, error) {
	return g.AcquireN(ctx, 1)
}

// AcquireFor acquires one slot for tenantID, using the same cancellation,
// queue-limit and release rules as Acquire. A nil context panics. An empty ID
// returns ErrInvalidTenant before cancellation or capacity is checked.
// IDs are exact strings supplied by the caller. Use trusted, bounded IDs:
// this package neither authenticates tenants nor limits their string length.
func (g *Airlock) AcquireFor(ctx context.Context, tenantID string) (*Permit, error) {
	return g.AcquireNFor(ctx, tenantID, 1)
}

// AcquireN atomically acquires n tokens for the default tenant. A nil context
// panics. Nonpositive n returns ErrInvalidWeight; n greater than Capacity returns
// ErrWeightExceedsCapacity. Weight validation precedes cancellation checks.
// It never holds a partial allocation. The queue and cancellation rules of
// Acquire apply, and the returned Permit releases all n tokens at once.
func (g *Airlock) AcquireN(ctx context.Context, n int) (*Permit, error) {
	return g.acquire(ctx, "", n)
}

// AcquireNFor atomically acquires n tokens for tenantID. Validation checks nil
// context, then tenant ID, then weight, before cancellation or capacity.
// Tenant identity rules are the same as AcquireFor; all other rules are those
// of AcquireN.
func (g *Airlock) AcquireNFor(ctx context.Context, tenantID string, n int) (*Permit, error) {
	if ctx == nil {
		panic("airlock: nil context")
	}
	if tenantID == "" {
		return nil, ErrInvalidTenant
	}
	return g.acquire(ctx, tenantID, n)
}

// acquire implements validated local admission; empty tenant is reserved for the default tenant.
func (g *Airlock) acquire(ctx context.Context, tenant string, n int) (*Permit, error) {
	if ctx == nil {
		panic("airlock: nil context")
	}
	if n <= 0 {
		return nil, ErrInvalidWeight
	}
	g.mu.Lock()
	if n > g.sched.Capacity() {
		g.mu.Unlock()
		return nil, ErrWeightExceedsCapacity
	}
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if g.sched.TryAcquire(n) {
		g.mu.Unlock()
		return &Permit{owner: g, weight: n}, nil
	}
	w, err := g.sched.Enqueue(tenant, n)
	if err != nil {
		g.mu.Unlock()
		return nil, err
	}
	// Install notification before dispatch while still holding mu.
	w.Arm()
	g.sched.Dispatch()
	g.mu.Unlock()
	select {
	case <-w.Ready():
		if err := w.Err(); err != nil {
			return nil, err
		}
		return &Permit{owner: g, weight: n}, nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.Granted() {
			g.mu.Unlock()
			return &Permit{owner: g, weight: n}, nil
		}
		if err := w.Err(); err != nil {
			g.mu.Unlock()
			return nil, err
		}
		g.sched.Remove(w)
		g.sched.Dispatch()
		g.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Release returns all tokens held by the permit and dispatches waiting requests.
// Subsequent calls, including concurrent calls, are no-ops. A nil or zero Permit
// is a no-op.
func (p *Permit) Release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		g := p.owner
		if g == nil {
			return
		}
		g.mu.Lock()
		g.sched.Release(p.weight)
		g.sched.Dispatch()
		g.mu.Unlock()
		p.owner = nil // a released permit need not retain the Airlock and its queues
	})
}

// Resize atomically changes the local token limit and dispatches waiting requests
// using the existing fairness policy. Nonpositive capacity returns ErrInvalidConfig
// without changing state. Setting the current capacity is a no-op.
//
// Shrinking never revokes permits: InUse may exceed Capacity until holders release.
// Queued requests heavier than the new capacity fail with ErrWeightExceedsCapacity;
// remaining requests retain their order and any surviving protected head.
// Cancellation, rejection and grant are arbitrated under the same mutex: whichever
// removes a waiter first determines its result.
//
// Resize waits for the internal mutex, not for existing permits to drain. Concurrent
// calls take effect in mutex acquisition order. Queue limits and priority are unchanged.
func (g *Airlock) Resize(capacity int) error {
	if capacity <= 0 {
		return ErrInvalidConfig
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sched.Resize(capacity)
}

// Stats returns a consistent snapshot, not a completion barrier.
func (g *Airlock) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sched.Stats()
}

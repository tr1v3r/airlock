package scheduler

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var (
	// ErrInvalidConfig reports invalid limits or a priority policy.
	ErrInvalidConfig = errors.New("airlock: invalid configuration")
	// ErrInvalidTenant reports an empty explicitly supplied tenant ID.
	ErrInvalidTenant = errors.New("airlock: empty tenant")
	// ErrInvalidWeight reports a nonpositive requested token count.
	ErrInvalidWeight = errors.New("airlock: invalid weight")
	// ErrWeightExceedsCapacity reports a request larger than the fixed capacity.
	ErrWeightExceedsCapacity = errors.New("airlock: weight exceeds capacity")
	// ErrQueueFull reports that the global waiting queue is full.
	ErrQueueFull = errors.New("airlock: queue full")
	// ErrGroupQueueFull reports that the tenant's priority group waiting queue is full.
	ErrGroupQueueFull = errors.New("airlock: priority group queue full")
	// ErrTenantQueueFull reports that the tenant's waiting queue is full.
	ErrTenantQueueFull = errors.New("airlock: tenant queue full")
	// ErrWouldBlock reports that TryAcquire cannot grant capacity immediately
	// without bypassing a queued request. The attempt does not join the queue.
	ErrWouldBlock = errors.New("airlock: acquisition would block")
)

// Config defines fixed limits for an Airlock. Queue limits count pending
// requests, not granted permits. Zero queue limits disable waiting.
type Config struct {
	// Capacity is the positive maximum number of held tokens.
	Capacity int
	// MaxQueued is the nonnegative maximum number of pending requests.
	MaxQueued int
	// MaxQueuedPerTenant is nonnegative and must not exceed MaxQueued.
	// It does not limit a tenant's number of held tokens or permits.
	MaxQueuedPerTenant int
	// Priority optionally enables fixed weighted selection between groups of
	// tenants. Nil preserves a single group with tenant round-robin scheduling.
	Priority *PriorityPolicy
}

// Stats is a consistent snapshot, not a completion barrier. Capacity and InUse
// count tokens; ActivePermits counts granted, unreleased permits, and Queued
// counts pending requests, including a protected head request.
type Stats struct {
	// Capacity is the fixed token limit configured at construction.
	Capacity int
	// InUse counts held tokens, including all tokens in each weighted permit.
	InUse int
	// ActivePermits counts grants not yet released, regardless of their weights.
	ActivePermits int
	// Queued counts pending requests, including a protected head waiting for its full weight.
	Queued int
	// WaitingTenants counts distinct tenants with pending requests, including the default tenant.
	WaitingTenants int
}

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
	// mu protects all mutable scheduling and accounting state.
	mu sync.Mutex
	// cfg holds immutable limits; Priority is compiled into priority rather than retained.
	Cfg Config
	// used counts held tokens; queued counts pending requests across all tenants.
	Used, Queued int
	// active counts granted allocations awaiting release.
	Active int
	// protected is the selected head that no later request may bypass.
	Protected *Waiter
	// tenants indexes nonempty waiting queues; empty keys represent the default tenant.
	Tenants map[string]*TenantQueue
	// priority owns group budgets and the weighted selection cursor.
	Priority PriorityState
}

// TenantQueue connects one nonempty tenant FIFO to its group rotation.
type TenantQueue struct {
	// id is the exact tenant key, empty only for the default tenant.
	ID string
	// Group owns this tenant's waiting budget and rotation.
	Group *PriorityQueue
	// turn is this queue's node in the group rotation.
	Turn *list.Element
	// requests stores pending waiters in FIFO order, including any protected head.
	Requests list.List
}

// Waiter tracks one pending atomic allocation under the owning Airlock mutex.
type Waiter struct {
	// ready closes after a full grant; cancellation removes the waiter without closing it.
	Ready chan struct{}
	// granted records the winning grant transition under the Airlock mutex.
	Granted bool
	// weight is the positive token count requested atomically.
	Weight int
	// node links the waiter into its tenant's FIFO.
	Node *list.Element
	// tenant owns the waiter's queue and group accounting.
	Tenant *TenantQueue
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
// priority policy. It snapshots the policy. It has no unlimited mode; limits,
// tenant assignments and selection shares cannot be changed after creation.
func New(cfg Config) (*Airlock, error) {
	if cfg.Capacity <= 0 || cfg.MaxQueued < 0 || cfg.MaxQueuedPerTenant < 0 || cfg.MaxQueuedPerTenant > cfg.MaxQueued {
		return nil, ErrInvalidConfig
	}
	priority, err := newPriorityState(cfg.Priority, cfg.MaxQueued)
	if err != nil {
		return nil, err
	}
	// Retain only the compiled immutable policy, not caller-owned collections.
	cfg.Priority = nil
	return &Airlock{Cfg: cfg, Tenants: make(map[string]*TenantQueue), Priority: priority}, nil
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
	if n > g.Cfg.Capacity {
		return nil, ErrWeightExceedsCapacity
	}
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if n <= g.Cfg.Capacity-g.Used && g.Queued == 0 {
		g.Used += n
		g.Active++
		g.mu.Unlock()
		return &Permit{owner: g, weight: n}, nil
	}
	w, err := g.Enqueue(tenant, n)
	if err != nil {
		g.mu.Unlock()
		return nil, err
	}
	g.Dispatch()
	g.mu.Unlock()
	select {
	case <-w.Ready:
		return &Permit{owner: g, weight: n}, nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.Granted {
			g.mu.Unlock()
			return &Permit{owner: g, weight: n}, nil
		}
		g.Remove(w)
		g.Dispatch()
		g.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Enqueue shares admission and queue accounting with the durable state machine.
// The caller serializes access and handles immediate admission first.
func (g *Airlock) Enqueue(tenant string, n int) (*Waiter, error) {
	if g.Queued >= g.Cfg.MaxQueued {
		return nil, ErrQueueFull
	}
	group := g.Priority.Group(tenant)
	if group.Queued >= group.Limit {
		return nil, ErrGroupQueueFull
	}
	tq := g.Tenants[tenant]
	if g.Cfg.MaxQueuedPerTenant == 0 || (tq != nil && tq.Requests.Len() >= g.Cfg.MaxQueuedPerTenant) {
		return nil, ErrTenantQueueFull
	}
	if tq == nil {
		tq = &TenantQueue{ID: tenant, Group: group}
		tq.Turn = group.Rotation.PushBack(tq)
		g.Tenants[tenant] = tq
	}
	w := &Waiter{Ready: make(chan struct{}), Tenant: tq, Weight: n}
	w.Node = tq.Requests.PushBack(w)
	g.Queued++
	group.Queued++
	return w, nil
}

// Remove requires mu. Empty tenant indexes are reclaimed immediately.
func (g *Airlock) Remove(w *Waiter) {
	if g.Protected == w {
		g.Protected = nil
	}
	tq := w.Tenant
	tq.Requests.Remove(w.Node)
	g.Queued--
	tq.Group.Queued--
	if tq.Requests.Len() == 0 {
		tq.Group.Rotation.Remove(tq.Turn)
		delete(g.Tenants, tq.ID)
	}
}

// Dispatch grants whole requests in selection order until capacity blocks the protected head.
// The caller must serialize access; a selected turn is consumed even if later canceled.
func (g *Airlock) Dispatch() {
	for g.Used < g.Cfg.Capacity && g.Queued > 0 {
		if g.Protected == nil {
			group := g.Priority.next()
			tq := group.Rotation.Front().Value.(*TenantQueue)
			g.Protected = tq.Requests.Front().Value.(*Waiter)
			// Selection consumes this tenant's turn, even if it later cancels.
			// Keep its queue indexed until the selected request is removed.
			group.Rotation.MoveToBack(tq.Turn)
		}
		w := g.Protected
		if w.Weight > g.Cfg.Capacity-g.Used {
			return
		}
		g.Remove(w)
		g.Used += w.Weight
		g.Active++
		w.Granted = true
		close(w.Ready)
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
		g.Used -= p.weight
		g.Active--
		g.Dispatch()
		g.mu.Unlock()
		p.owner = nil // a released permit need not retain the Airlock and its queues
	})
}

// Stats returns a consistent snapshot, not a completion barrier.
func (g *Airlock) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Stats{Capacity: g.Cfg.Capacity, InUse: g.Used, ActivePermits: g.Active, Queued: g.Queued, WaitingTenants: len(g.Tenants)}
}

package airlock

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
	// ErrWeightExceedsCapacity reports a request larger than the current capacity.
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

// Config defines initial capacity and fixed queue limits for an Airlock.
// Queue limits count pending requests, not granted permits. Zero queue limits
// disable waiting.
type Config struct {
	// Capacity is the positive initial token limit; Resize can change it.
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
	// Capacity is the current token limit; InUse may exceed it after shrinking.
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
	// cfg holds the current capacity and fixed queue limits; mu protects capacity.
	// Priority is compiled into priority rather than retained.
	cfg Config
	// used counts held tokens; queued counts pending requests across all tenants.
	used, queued int
	// active counts granted allocations awaiting release.
	active int
	// protected is the selected head that no later request may bypass.
	protected *waiter
	// tenants indexes nonempty waiting queues; empty keys represent the default tenant.
	tenants map[string]*tenantQueue
	// priority owns group budgets and the weighted selection cursor.
	priority priorityState
}

// tenantQueue connects one nonempty tenant FIFO to its group rotation.
type tenantQueue struct {
	// id is the exact tenant key, empty only for the default tenant.
	id string
	// group owns this tenant's waiting budget and rotation.
	group *priorityQueue
	// turn is this queue's node in the group rotation.
	turn *list.Element
	// requests stores pending waiters in FIFO order, including any protected head.
	requests list.List
}

// waiter tracks one pending atomic allocation under the owning Airlock mutex.
type waiter struct {
	// ready closes after a full grant or resize rejection; cancellation does not close it.
	ready chan struct{}
	// granted records the winning grant transition under the Airlock mutex.
	granted bool
	// err records a terminal resize rejection, published by closing ready.
	err error
	// weight is the positive token count requested atomically.
	weight int
	// node links the waiter into its tenant's FIFO.
	node *list.Element
	// tenant owns the waiter's queue and group accounting.
	tenant *tenantQueue
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
	if cfg.Capacity <= 0 || cfg.MaxQueued < 0 || cfg.MaxQueuedPerTenant < 0 || cfg.MaxQueuedPerTenant > cfg.MaxQueued {
		return nil, ErrInvalidConfig
	}
	priority, err := newPriorityState(cfg.Priority, cfg.MaxQueued)
	if err != nil {
		return nil, err
	}
	// Retain only the compiled immutable policy, not caller-owned collections.
	cfg.Priority = nil
	return &Airlock{cfg: cfg, tenants: make(map[string]*tenantQueue), priority: priority}, nil
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
	if n > g.cfg.Capacity {
		g.mu.Unlock()
		return nil, ErrWeightExceedsCapacity
	}
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if n <= g.cfg.Capacity-g.used && g.queued == 0 {
		g.used += n
		g.active++
		g.mu.Unlock()
		return &Permit{owner: g, weight: n}, nil
	}
	w, err := g.enqueue(tenant, n)
	if err != nil {
		g.mu.Unlock()
		return nil, err
	}
	g.dispatch()
	g.mu.Unlock()
	select {
	case <-w.ready:
		if w.err != nil {
			return nil, w.err
		}
		return &Permit{owner: g, weight: n}, nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.granted {
			g.mu.Unlock()
			return &Permit{owner: g, weight: n}, nil
		}
		if w.err != nil {
			g.mu.Unlock()
			return nil, w.err
		}
		g.remove(w)
		g.dispatch()
		g.mu.Unlock()
		return nil, ctx.Err()
	}
}

// enqueue shares admission and queue accounting with the durable state machine.
// The caller serializes access and handles immediate admission first.
func (g *Airlock) enqueue(tenant string, n int) (*waiter, error) {
	if g.queued >= g.cfg.MaxQueued {
		return nil, ErrQueueFull
	}
	group := g.priority.group(tenant)
	if group.queued >= group.limit {
		return nil, ErrGroupQueueFull
	}
	tq := g.tenants[tenant]
	if g.cfg.MaxQueuedPerTenant == 0 || (tq != nil && tq.requests.Len() >= g.cfg.MaxQueuedPerTenant) {
		return nil, ErrTenantQueueFull
	}
	if tq == nil {
		tq = &tenantQueue{id: tenant, group: group}
		tq.turn = group.rotation.PushBack(tq)
		g.tenants[tenant] = tq
	}
	w := &waiter{ready: make(chan struct{}), tenant: tq, weight: n}
	w.node = tq.requests.PushBack(w)
	g.queued++
	group.queued++
	return w, nil
}

// remove requires mu. Empty tenant indexes are reclaimed immediately.
func (g *Airlock) remove(w *waiter) {
	if g.protected == w {
		g.protected = nil
	}
	tq := w.tenant
	tq.requests.Remove(w.node)
	g.queued--
	tq.group.queued--
	if tq.requests.Len() == 0 {
		tq.group.rotation.Remove(tq.turn)
		delete(g.tenants, tq.id)
	}
}

// dispatch grants whole requests in selection order until capacity blocks the protected head.
// The caller must serialize access; a selected turn is consumed even if later canceled.
func (g *Airlock) dispatch() {
	for g.used < g.cfg.Capacity && g.queued > 0 {
		if g.protected == nil {
			group := g.priority.next()
			tq := group.rotation.Front().Value.(*tenantQueue)
			g.protected = tq.requests.Front().Value.(*waiter)
			// Selection consumes this tenant's turn, even if it later cancels.
			// Keep its queue indexed until the selected request is removed.
			group.rotation.MoveToBack(tq.turn)
		}
		w := g.protected
		if w.weight > g.cfg.Capacity-g.used {
			return
		}
		g.remove(w)
		g.used += w.weight
		g.active++
		w.granted = true
		close(w.ready)
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
		g.used -= p.weight
		g.active--
		g.dispatch()
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
	if capacity == g.cfg.Capacity {
		return nil
	}
	previous := g.cfg.Capacity
	g.cfg.Capacity = capacity
	if capacity < previous {
		for _, tenant := range g.tenants {
			for node := tenant.requests.Front(); node != nil; {
				next := node.Next()
				w := node.Value.(*waiter)
				if w.weight > capacity {
					g.remove(w)
					w.err = ErrWeightExceedsCapacity
					close(w.ready)
				}
				node = next
			}
		}
	}
	g.dispatch()
	return nil
}

// Stats returns a consistent snapshot, not a completion barrier.
func (g *Airlock) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Stats{Capacity: g.cfg.Capacity, InUse: g.used, ActivePermits: g.active, Queued: g.queued, WaitingTenants: len(g.tenants)}
}

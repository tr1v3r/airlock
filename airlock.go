package airlock

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var (
	// ErrInvalidConfig reports an invalid capacity or queue limit.
	ErrInvalidConfig = errors.New("airlock: invalid configuration")
	// ErrInvalidTenant reports an empty explicit ID or more than one tenant ID.
	ErrInvalidTenant = errors.New("airlock: expected zero or one nonempty tenant ID")
	// ErrQueueFull reports that the global waiting queue is full.
	ErrQueueFull = errors.New("airlock: queue full")
	// ErrTenantQueueFull reports that the tenant's waiting queue is full.
	ErrTenantQueueFull = errors.New("airlock: tenant queue full")
)

// Config defines fixed limits for an Airlock. Queue limits count pending
// requests, not granted permits. Zero queue limits disable waiting.
type Config struct {
	// Capacity is the positive maximum number of held permits.
	Capacity int
	// MaxQueued is the nonnegative maximum number of pending requests.
	MaxQueued int
	// MaxQueuedPerTenant is nonnegative and must not exceed MaxQueued.
	// It does not limit a tenant's number of held permits.
	MaxQueuedPerTenant int
}

// Stats is a consistent snapshot, not a completion barrier.
type Stats struct {
	Capacity       int
	InUse          int
	Queued         int
	WaitingTenants int
}

// Airlock controls single-token admission with FIFO waiting within each tenant
// and round-robin admission across nonempty tenant queues. New waiting tenants
// join the end of the rotation. It does not start workers or track completion.
//
// Fairness applies to queued requests, not execution time or requests rejected
// before queueing. Running work is never preempted. One tenant may hold every
// slot when others are idle. Use New; the zero value is not usable.
// An Airlock is safe for concurrent use and must not be copied.
type Airlock struct {
	mu           sync.Mutex
	cfg          Config
	used, queued int
	tenants      map[string]*tenantQueue
	rotation     list.List
}

type tenantQueue struct {
	id       string
	turn     *list.Element
	requests list.List
}

type waiter struct {
	ready   chan struct{}
	granted bool
	node    *list.Element
	tenant  *tenantQueue
}

// Permit must not be copied. Release is idempotent, including concurrent calls.
// Cancellation after a successful Acquire does not release it automatically.
// Obtain permits from Airlock.Acquire; a zero-value Permit holds no capacity.
type Permit struct {
	once  sync.Once
	owner *Airlock
}

// New constructs an Airlock, or returns ErrInvalidConfig for invalid limits.
// It has no unlimited mode. Limits cannot be changed after creation.
func New(cfg Config) (*Airlock, error) {
	if cfg.Capacity <= 0 || cfg.MaxQueued < 0 || cfg.MaxQueuedPerTenant < 0 || cfg.MaxQueuedPerTenant > cfg.MaxQueued {
		return nil, ErrInvalidConfig
	}
	return &Airlock{cfg: cfg, tenants: make(map[string]*tenantQueue)}, nil
}

// Acquire atomically acquires one slot or returns an error without holding any.
// The first state transition under the lock wins cancellation/grant races.
// A grant that wins may return success even after ctx is cancelled; the caller
// must release every successful permit. Cancellation does not stop running work.
//
// Omitting tenantID uses a shared default tenant, distinct from every explicit
// tenant. It obeys the same queue limits and scheduling rules as named tenants.
// A nil context panics. An empty explicit ID or more than one ID returns
// ErrInvalidTenant. Next, an already-cancelled context returns ctx.Err().
// If capacity cannot be granted,
// global and then per-tenant queue limits are checked; a full global queue takes
// precedence over a full tenant queue. Queue rejection does not wait for ctx.
//
// Tenant IDs are exact strings supplied by the caller. Use trusted, bounded IDs:
// this package neither authenticates tenants nor limits their string length.
func (g *Airlock) Acquire(ctx context.Context, tenantID ...string) (*Permit, error) {
	if ctx == nil {
		panic("airlock: nil context")
	}
	if len(tenantID) > 1 || (len(tenantID) == 1 && tenantID[0] == "") {
		return nil, ErrInvalidTenant
	}
	// Empty is reserved internally for the default tenant; callers cannot name it.
	tenant := ""
	if len(tenantID) == 1 {
		tenant = tenantID[0]
	}
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if g.used < g.cfg.Capacity && g.queued == 0 {
		g.used++
		g.mu.Unlock()
		return &Permit{owner: g}, nil
	}
	if g.queued >= g.cfg.MaxQueued {
		g.mu.Unlock()
		return nil, ErrQueueFull
	}
	tq := g.tenants[tenant]
	if g.cfg.MaxQueuedPerTenant == 0 || (tq != nil && tq.requests.Len() >= g.cfg.MaxQueuedPerTenant) {
		g.mu.Unlock()
		return nil, ErrTenantQueueFull
	}
	if tq == nil {
		tq = &tenantQueue{id: tenant}
		tq.turn = g.rotation.PushBack(tq)
		g.tenants[tenant] = tq
	}
	w := &waiter{ready: make(chan struct{}), tenant: tq}
	w.node = tq.requests.PushBack(w)
	g.queued++
	g.mu.Unlock()
	select {
	case <-w.ready:
		return &Permit{owner: g}, nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.granted {
			g.mu.Unlock()
			return &Permit{owner: g}, nil
		}
		g.remove(w)
		g.dispatch()
		g.mu.Unlock()
		return nil, ctx.Err()
	}
}

// remove requires mu. Empty tenant indexes are reclaimed immediately.
func (g *Airlock) remove(w *waiter) {
	tq := w.tenant
	tq.requests.Remove(w.node)
	g.queued--
	if tq.requests.Len() == 0 {
		g.rotation.Remove(tq.turn)
		delete(g.tenants, tq.id)
	}
}

func (g *Airlock) dispatch() {
	for g.used < g.cfg.Capacity && g.queued > 0 {
		tq := g.rotation.Front().Value.(*tenantQueue)
		w := tq.requests.Front().Value.(*waiter)
		g.remove(w)
		if tq.requests.Len() > 0 {
			g.rotation.MoveToBack(tq.turn)
		}
		g.used++
		w.granted = true
		close(w.ready)
	}
}

// Release returns the permit once and dispatches waiting requests. Subsequent
// calls, including concurrent calls, are no-ops. A nil or zero Permit is a no-op.
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
		g.used--
		g.dispatch()
		g.mu.Unlock()
		p.owner = nil // a released permit need not retain the Airlock and its queues
	})
}

// Stats returns a consistent snapshot, not a completion barrier.
func (g *Airlock) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Stats{Capacity: g.cfg.Capacity, InUse: g.used, Queued: g.queued, WaitingTenants: len(g.tenants)}
}

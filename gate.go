package airlock

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var (
	// ErrInvalidGateConfig reports an invalid capacity or queue limit.
	ErrInvalidGateConfig = errors.New("airlock: invalid gate configuration")
	// ErrInvalidTenant reports an empty tenant ID.
	ErrInvalidTenant = errors.New("airlock: empty tenant")
	// ErrQueueFull reports that the global waiting queue is full.
	ErrQueueFull = errors.New("airlock: queue full")
	// ErrTenantQueueFull reports that the tenant's waiting queue is full.
	ErrTenantQueueFull = errors.New("airlock: tenant queue full")
)

// GateConfig defines fixed limits for a Gate. Queue limits count pending
// requests, not granted permits. Zero queue limits disable waiting.
type GateConfig struct {
	// Capacity is the positive maximum number of held permits.
	Capacity int
	// MaxQueued is the nonnegative maximum number of pending requests.
	MaxQueued int
	// MaxQueuedPerTenant is nonnegative and must not exceed MaxQueued.
	// It does not limit a tenant's number of held permits.
	MaxQueuedPerTenant int
}

// GateStats is a consistent snapshot, not a completion barrier.
type GateStats struct {
	Capacity       int
	InUse          int
	Queued         int
	WaitingTenants int
}

// Gate controls single-token admission with FIFO waiting within each tenant
// and round-robin admission across nonempty tenant queues. New waiting tenants
// join the end of the rotation. It does not start workers or track completion.
//
// Fairness applies to queued requests, not execution time or requests rejected
// before queueing. Running work is never preempted. One tenant may hold every
// slot when others are idle. Use NewGate; the zero value is not usable.
// A Gate is safe for concurrent use and must not be copied.
type Gate struct {
	mu           sync.Mutex
	cfg          GateConfig
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
// Obtain permits from Gate.Acquire; a zero-value Permit holds no capacity.
type Permit struct {
	once sync.Once
	gate *Gate
}

// NewGate constructs a Gate, or returns ErrInvalidGateConfig for invalid limits.
// Unlike NewPool, it has no unlimited mode. Limits cannot be changed after creation.
func NewGate(cfg GateConfig) (*Gate, error) {
	if cfg.Capacity <= 0 || cfg.MaxQueued < 0 || cfg.MaxQueuedPerTenant < 0 || cfg.MaxQueuedPerTenant > cfg.MaxQueued {
		return nil, ErrInvalidGateConfig
	}
	return &Gate{cfg: cfg, tenants: make(map[string]*tenantQueue)}, nil
}

// Acquire atomically acquires one slot or returns an error without holding any.
// The first state transition under the lock wins cancellation/grant races.
// A grant that wins may return success even after ctx is cancelled; the caller
// must release every successful permit. Cancellation does not stop running work.
//
// A nil context panics. Empty tenant IDs return ErrInvalidTenant. Next, an
// already-cancelled context returns ctx.Err(). If capacity cannot be granted,
// global and then per-tenant queue limits are checked; a full global queue takes
// precedence over a full tenant queue. Queue rejection does not wait for ctx.
//
// Tenant IDs are exact strings supplied by the caller. Use trusted, bounded IDs:
// this package neither authenticates tenants nor limits their string length.
func (g *Gate) Acquire(ctx context.Context, tenant string) (*Permit, error) {
	if ctx == nil {
		panic("airlock: nil context")
	}
	if tenant == "" {
		return nil, ErrInvalidTenant
	}
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if g.used < g.cfg.Capacity && g.queued == 0 {
		g.used++
		g.mu.Unlock()
		return &Permit{gate: g}, nil
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
		return &Permit{gate: g}, nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.granted {
			g.mu.Unlock()
			return &Permit{gate: g}, nil
		}
		g.remove(w)
		g.dispatch()
		g.mu.Unlock()
		return nil, ctx.Err()
	}
}

// remove requires mu. Empty tenant indexes are reclaimed immediately.
func (g *Gate) remove(w *waiter) {
	tq := w.tenant
	tq.requests.Remove(w.node)
	g.queued--
	if tq.requests.Len() == 0 {
		g.rotation.Remove(tq.turn)
		delete(g.tenants, tq.id)
	}
}

func (g *Gate) dispatch() {
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
		g := p.gate
		if g == nil {
			return
		}
		g.mu.Lock()
		g.used--
		g.dispatch()
		g.mu.Unlock()
		p.gate = nil // a released permit need not retain the Gate and its queues
	})
}

// Stats returns a consistent snapshot, not a completion barrier.
func (g *Gate) Stats() GateStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return GateStats{Capacity: g.cfg.Capacity, InUse: g.used, Queued: g.queued, WaitingTenants: len(g.tenants)}
}

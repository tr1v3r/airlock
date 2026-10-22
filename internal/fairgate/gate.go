// Package fairgate is an experimental tenant admission controller for RFC 001.
// It is internal deliberately: its API is not a supported Airlock public API.
package fairgate

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var (
	ErrInvalidTenant   = errors.New("airlock: empty tenant")
	ErrQueueFull       = errors.New("airlock: queue full")
	ErrTenantQueueFull = errors.New("airlock: tenant queue full")
)

type Config struct {
	Capacity           int
	MaxQueued          int
	MaxQueuedPerTenant int
}

type Stats struct {
	Capacity       int
	InUse          int
	Queued         int
	WaitingTenants int
}

// Gate must be constructed with New and must not be copied.
// It owns no workers. Fairness applies to waiting requests, not execution time.
type Gate struct {
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
type Permit struct {
	once sync.Once
	gate *Gate
}

func New(cfg Config) (*Gate, error) {
	if cfg.Capacity <= 0 || cfg.MaxQueued < 0 || cfg.MaxQueuedPerTenant < 0 || cfg.MaxQueuedPerTenant > cfg.MaxQueued {
		return nil, errors.New("airlock: invalid gate configuration")
	}
	return &Gate{cfg: cfg, tenants: make(map[string]*tenantQueue)}, nil
}

// Acquire atomically acquires one slot or returns an error without holding any.
// The first state transition under the lock wins cancellation/grant races.
// A nil context panics; tenant IDs must be trusted and nonempty.
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

func (p *Permit) Release() {
	p.once.Do(func() {
		g := p.gate
		g.mu.Lock()
		g.used--
		g.dispatch()
		g.mu.Unlock()
	})
}

// Stats returns a consistent snapshot, not a completion barrier.
func (g *Gate) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Stats{g.cfg.Capacity, g.used, g.queued, len(g.tenants)}
}

package airlock

// TryAcquire atomically acquires one token for the shared default tenant without
// joining a queue or waiting for capacity. If capacity is insufficient or any
// request is queued, it returns a nil permit and ErrWouldBlock without changing
// admission state. It never bypasses waiting requests, even when tokens are idle.
//
// Queue limits do not affect TryAcquire. The method has no context because it
// does not wait for capacity, but it still acquires the internal mutex and is
// not wait-free. A successful permit must be released, as with Acquire.
func (g *Airlock) TryAcquire() (*Permit, error) {
	return g.TryAcquireN(1)
}

// TryAcquireFor attempts to acquire one token for tenantID, with the same rules
// as TryAcquire. An empty ID returns ErrInvalidTenant before checking capacity.
// Tenant IDs follow the same trust and identity rules as AcquireFor.
func (g *Airlock) TryAcquireFor(tenantID string) (*Permit, error) {
	return g.TryAcquireNFor(tenantID, 1)
}

// TryAcquireN attempts to acquire all n tokens for the default tenant, with the
// same rules as TryAcquire. Nonpositive n returns ErrInvalidWeight; n greater
// than Capacity returns ErrWeightExceedsCapacity. Validation precedes checking
// availability. Failure never holds a partial allocation. The returned permit
// releases all n tokens together.
func (g *Airlock) TryAcquireN(n int) (*Permit, error) {
	if n <= 0 {
		return nil, ErrInvalidWeight
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if n > g.cfg.Capacity {
		return nil, ErrWeightExceedsCapacity
	}
	if g.queued != 0 || n > g.cfg.Capacity-g.used {
		return nil, ErrWouldBlock
	}
	g.used += n
	g.active++
	return &Permit{owner: g, weight: n}, nil
}

// TryAcquireNFor attempts to acquire all n tokens for tenantID. It validates the
// nonempty tenant ID before weight or capacity, then follows TryAcquireN. Like
// TryAcquireFor, it neither queues nor changes a tenant's rotation position.
func (g *Airlock) TryAcquireNFor(tenantID string, n int) (*Permit, error) {
	if tenantID == "" {
		return nil, ErrInvalidTenant
	}
	return g.TryAcquireN(n)
}

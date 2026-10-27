package airlock

// TryAcquire atomically acquires one token for the shared default tenant without
// joining a queue or waiting for capacity. If capacity is insufficient or any
// request is queued, it returns a nil permit and ErrWouldBlock without changing
// admission state. It never bypasses waiting requests, even when tokens are idle.
//
// Queue limits do not affect TryAcquire. The method has no context because it
// does not wait for capacity, but it still acquires the internal mutex and is
// not wait-free. A successful permit must be released, as with Acquire.
func (a *Airlock) TryAcquire() (*Permit, error) {
	return a.TryAcquireN(1)
}

// TryAcquireFor attempts to acquire one token for tenantID, with the same rules
// as TryAcquire. An empty ID returns ErrInvalidTenant before checking capacity.
// Tenant IDs follow the same trust and identity rules as AcquireFor.
func (a *Airlock) TryAcquireFor(tenantID string) (*Permit, error) {
	return a.TryAcquireNFor(tenantID, 1)
}

// TryAcquireN attempts to acquire all n tokens for the default tenant, with the
// same rules as TryAcquire. Nonpositive n returns ErrInvalidWeight; n greater
// than Capacity returns ErrWeightExceedsCapacity. Validation precedes checking
// availability. Failure never holds a partial allocation. The returned permit
// releases all n tokens together.
func (a *Airlock) TryAcquireN(n int) (*Permit, error) {
	if n <= 0 {
		return nil, ErrInvalidWeight
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if n > a.cfg.Capacity {
		return nil, ErrWeightExceedsCapacity
	}
	if a.queued != 0 || n > a.cfg.Capacity-a.used {
		return nil, ErrWouldBlock
	}

	a.used += n
	a.active++
	return &Permit{owner: a, weight: n}, nil
}

// TryAcquireNFor attempts to acquire all n tokens for tenantID. It validates the
// nonempty tenant ID before weight or capacity, then follows TryAcquireN. Like
// TryAcquireFor, it neither queues nor changes a tenant's rotation position.
func (a *Airlock) TryAcquireNFor(tenantID string, n int) (*Permit, error) {
	if tenantID == "" {
		return nil, ErrInvalidTenant
	}
	return a.TryAcquireN(n)
}

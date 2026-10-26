package airlock

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

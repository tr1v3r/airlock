// Package airlock provides context-aware, atomic weighted admission with bounded
// waiting queues and round-robin scheduling between tenants. Optional PriorityPolicy
// assigns tenants to groups with weighted request-selection turns and reserved
// waiting budgets. Shares count selections, not tokens or execution time; they
// are not strict priority or preemption. A selected request
// waits for its full weight without allowing smaller or higher-priority requests
// to bypass it. Accepted, uncancelled requests make progress provided holders
// eventually release and scheduling progresses; there is no fixed latency bound.
// TryAcquire methods never queue or wait for capacity, but still take the internal
// mutex. They return ErrWouldBlock when capacity is insufficient or any request
// is waiting, preserving the same no-bypass rule.
// Use New for process-local admission. Its Permits have no lease or automatic
// expiry; cancellation after a successful acquisition does not release capacity.
// Callers own worker execution, completion and release.
//
// NewShared instead uses an injected Driver and returns expiring Leases. Callers
// explicitly renew them, stop work on invalidation, then release. Lease expiry
// reclaims accounting but cannot stop work or fence downstream writes. Transport
// errors never authorize execution and may require recovery using the original
// request identity. Driver modules document their storage and failover limits.
// Runtime dependencies of the core package are standard library only.
package airlock

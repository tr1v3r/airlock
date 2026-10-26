// Package airlock is the application entry point for admission control: local,
// in-process admission with New(Config), and shared, driver-backed admission
// with NewShared(Driver, SharedOptions). The package provides context-aware,
// atomic weighted admission with bounded waiting queues and round-robin
// scheduling between tenants. Optional PriorityPolicy assigns tenants to groups
// with weighted request-selection turns and reserved waiting budgets. Shares
// count selections, not tokens or execution time; they are not strict priority
// or preemption. A selected request waits for its full weight without allowing
// smaller or higher-priority requests to bypass it. Accepted requests not
// cancelled or rejected by Resize make progress provided holders eventually
// release and scheduling progresses; there is no fixed latency bound.
// TryAcquire methods never queue or wait for capacity, but still take the
// internal mutex. They return ErrWouldBlock when capacity is insufficient or
// any request is waiting, preserving the same no-bypass rule.
//
// Use New for process-local admission. Its Permits have no lease or automatic
// expiry; cancellation after a successful acquisition does not release capacity.
// Resize changes local capacity without revoking existing permits; shrinking
// rejects queued requests heavier than the new capacity. InUse may temporarily
// exceed Capacity until holders release.
// Callers own worker execution, completion and release.
//
// NewShared instead uses an injected protocol.Driver and returns expiring
// Leases. Callers explicitly renew them, stop work on invalidation, then
// release. Lease expiry reclaims accounting but cannot stop work or fence
// downstream writes. Transport errors never authorize execution and may require
// recovery using the original request identity. The shared admission protocol,
// its reference state machine and the driver contract live in the protocol
// subpackage; the scheduling core shared by local and shared admission is
// internal. Shared admission capacity stays immutable; Resize is local only.
// Driver modules document their storage and failover limits.
// Runtime dependencies of this package are standard library only.
package airlock

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
// Callers own worker execution and completion. Permits are local to the process;
// they have no lease or automatic expiry. Runtime dependencies are standard library only.
package airlock

// Package airlock provides context-aware, atomic weighted admission with bounded
// waiting queues and round-robin scheduling between tenants. A selected request
// waits for its full weight without allowing smaller requests to bypass it.
// Callers own worker execution and completion. Permits are local to the process;
// they have no lease or automatic expiry. Runtime dependencies are standard library only.
package airlock

// Package airlock provides context-aware, single-token admission with bounded
// waiting queues and round-robin scheduling between tenants. Callers own worker
// execution and completion. Weighted acquisition is not implemented.
package airlock

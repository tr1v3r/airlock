# Implementing a custom driver

[简体中文](drivers.zh-CN.md) · [Shared user guide](shared.md)

This page is for backend adapter authors. Business callers normally use Shared's
Acquire methods and the returned Lease, without constructing protocol requests.

```go
type Driver interface {
    Submit(context.Context, Request) (Result, error)
    Observe(context.Context, Request) (Result, error)
    Cancel(context.Context, Request) (Result, error)
    Renew(context.Context, LeaseRef) (Result, error)
    Release(context.Context, LeaseRef) (Result, error)
}
```

| Operation | Contract |
| --- | --- |
| Submit | Atomically admit or enqueue the whole weight; retrying the same request does not refresh its TTL |
| Observe | Return the outcome, advancing due maintenance; read-only fast paths keep Revision |
| Cancel | Cancel a queued or granted request; cancellation may arrive before submission |
| Renew | Extend only a live matching lease; never revive an expired lease |
| Release | Return the matching lease's entire weight at most once |

Submit does not wait for capacity: it returns Queued or Granted (or an error).
Calls may still wait for backend I/O/locks, bounded by their context. Shared owns
polling and cancellation cleanup. It does not implicitly retry network failures.

Request contains resource, immutable configuration, ID, Owner, tenant, weight and
submission/waiting deadlines. Reuse it unchanged. LeaseRef contains that original
Request plus the granted LeaseID in its ID field. Result reports lifecycle status,
lease ID, weight, remaining lease duration and backend revision. An error never
authorizes work, even if the write may have committed.

## Reuse the state machine and preserve the atomic boundary

Storage layout is a driver choice: SQL tables, snapshots, or another representation
are all allowed when the same atomic contract holds. The reference helpers are optional.
All three drivers use one structured entry point:

```go
func Transition(state *SharedState, now time.Time, cmd Command) (
    next *SharedState, result Result, window CommitWindow, err error,
)
```

A nil input initializes a resource. Transition takes exclusive ownership of the
input maps/records, which must not be reused even on error. It performs no I/O.
Persist every nonnil returned state, including on business errors. PostgreSQL maps
SharedState and DriverRecord to relational rows; memory and Redis use DecodeState
and EncodeState around Transition to persist snapshots.

Command/Operation/ValidateCommand are implementation helpers, not the public
Driver call interface. Direct callers provide a complete ResourceConfig; only
NewShared resolves defaults.

The commit window includes original and resulting deadlines. In-place renewal
must retain the original lease expiry as a publication bound. Drivers with a
non-expiring lock may ignore the window while retaining exclusive access through
commit. An expiring update lease, as used by Redis, must atomically check owner
and window at publication. Redis does not compare the previous snapshot. Optimistic custom
drivers must instead compare their previously read state and enforce the window.
Known lock contention permits retrying acquisition without loading or computing
the full snapshot. Confirmed ownership/comparison/window misses permit rereading
and recomputing; transport errors remain ambiguous outcomes.

Redis bounds state work and lock contention with `WithOperationTimeout` (default
five seconds), or the caller's earlier deadline. After acquiring the lock, cleanup
uses an independent context with a budget of `min(operation timeout, 100ms)`, so
return can follow the deadline by up to that additional budget. Owner-checked
unlock failure leaves TTL cleanup; it neither undoes committed state nor makes an
uncertain commit successful. The final operation-context check still applies.

The reference state machine folds boundary collection into its existing record
passes, preserving the same window; see the [equivalence and benchmark notes](../benchmarks/commit-window/README.md).

Serialize the whole resource's read, transition and commit. Use backend time after
obtaining exclusive access; optimistic backends must enforce the time window
returned by Transition in the atomic publication step. Commit a non-nil
next state even with a business error: rejection records and expiry/scheduling
changes are still authoritative. Return a usable result only after successful
commit, and honor context cancellation. No fallback to a separate local pool.

The [memory reference](../../driver/memory/memory.go),
[PostgreSQL adapter](../../driver/postgres/postgres.go) and
[Redis adapter](../../driver/redis/redis.go) show three implementations.
Run the shared [conformance suite](../../internal/driverconformance/conformance.go)
plus backend-specific locking, deadline and uncertain-commit tests. Memory alone
cannot demonstrate persistence, network failure or failover behavior.

Read the full [contract and decisions](../decisions/005-driver-contract.md) before
implementing an adapter. [Module boundaries](../../MODULES.md) describe independent
module pins and verification. The old Execute(ctx, Command) driver interface is
removed; custom drivers implement the five methods above.

Drivers may use `Observation.Observe` and `SharedState.NextMaintenance` for compact
read-only results. Read metadata and the target request atomically using backend
time. Due maintenance, missing results or incomplete indexes must fall back to
full Observe so expiry-driven scheduling still progresses without Release.

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
| Observe | Return the outcome while advancing expiry and scheduling; this can write state |
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

`Transition(snapshot, backendNow, command)` computes a new snapshot, Result and
business error without I/O. Command/Operation/ValidateCommand are implementation
helpers, not the public Driver call interface. Direct callers must provide a
complete ResourceConfig; only NewShared resolves default configuration.

Serialize the whole resource's read, transition and commit. Use backend time after
obtaining exclusive access; optimistic backends must enforce the time window
returned by TransitionOptimistic in the atomic publication step. Commit a non-nil
next snapshot even with a business error: rejection records and expiry/scheduling
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

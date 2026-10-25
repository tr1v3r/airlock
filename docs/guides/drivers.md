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

The `protocol` package owns commands, lifecycle records and the reference state
machine. The root `airlock` package exposes the application API and aliases the
Driver contract. Both local admission and the protocol reuse `internal/scheduler`
for FIFO queues, tenant rotation, priority selection and protected heads.

```text
application -> airlock.Shared -> Driver business methods
                                  | read / lock or CAS
                                  v
                            protocol.Transition
                                  |
                            internal/scheduler
                                  ^
application -> airlock.Airlock ----|
```

All three drivers call the same entry point:

```go
func Transition(state *State, now time.Time, cmd Command) (
    next *State, result Result, window CommitWindow, err error,
)
```

Import `github.com/tr1v3r/airlock/protocol` to implement a driver. `State` contains
`Record` entries and the scheduling layout. A nil input initializes a resource.
The caller transfers exclusive ownership of all input maps and records; input
must not be reused even on error. Transition performs no I/O. Persist every
nonnil next state, including on business errors, and return a usable result only
after commit. A nil next state must not be persisted.

| Driver | Read and transition | Publication |
| --- | --- | --- |
| memory | Hold the resource gate; DecodeState, then Transition with local time | EncodeState and replace the snapshot before releasing the gate |
| PostgreSQL | Lock the resource row; load records; obtain database time; Transition | Write changed rows and commit while holding the transaction lock |
| Redis | Atomically read snapshot and server time; DecodeState, then Transition | EncodeState; Lua checks the exact original snapshot and CommitWindow before publishing |

`CommitWindow.NotBefore` is inclusive and `Before` is exclusive (zero means no
upper bound). The window includes original and resulting lease/queue deadlines,
submission validity boundaries and retention deadlines. In particular, renewal
must retain the original lease expiry as a publication bound. Redis retries only
confirmed CAS or time-window misses after a fresh read. A transport error is an
ambiguous outcome and must not be treated as a conflict. Lock-based drivers may
ignore the window while maintaining exclusive access through commit.

`DecodeState` and `EncodeState` are codecs, not state transitions. SQL drivers may
map State and Record directly to rows. Custom drivers can implement the same
business contract with their own state machine and storage representation.
`ValidateCommand` checks input before allocating resources or opening transactions.
Direct protocol callers supply a complete ResourceConfig; NewShared alone resolves
defaults. Honor context cancellation and never fall back to an independent pool.

The [memory reference](../../driver/memory/memory.go),
[PostgreSQL adapter](../../driver/postgres/postgres.go) and
[Redis adapter](../../driver/redis/redis.go) show three implementations.
Run the shared [conformance suite](../../internal/driverconformance/conformance.go)
plus backend-specific locking, deadline and uncertain-commit tests. Memory alone
cannot demonstrate persistence, network failure or failover behavior.

Read the full [contract and decisions](../decisions/005-driver-contract.md) before
implementing an adapter. [Module boundaries](../../MODULES.md) describe independent
module pins and verification.

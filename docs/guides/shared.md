# Shared admission: from configuration to cleanup

[简体中文](shared.zh-CN.md) · [Back to README](../../README.md)

Use `airlock.New` for ordinary admission within one process. Use `airlock.NewShared`
when processes need to share capacity through Redis or PostgreSQL. The memory
adapter is a reference for tests and demos of this lease API; it is not the local
admission entry point or a durable backend.

## 1. Choose a backend, then configure admission

Create a driver using the [Redis setup](../../driver/redis/README.md) or
[PostgreSQL setup](../../driver/postgres/README.md). The application owns its
connection client/pool. PostgreSQL schema creation is an explicit deployment step.
With that `driver`, construction is:

```go
shared, err := airlock.NewShared(driver, airlock.SharedOptions{
    Resource: "jobs",
    Config: airlock.ResourceConfig{
        Admission: airlock.Config{
            Capacity: 10, MaxQueued: 20, MaxQueuedPerTenant: 10,
        },
    },
})
if err != nil {
    return err
}
```

Resource and positive capacity are required. Both queue limits are optional:
zero disables waiting, just as with local `New`. `MaxWait` does not enable queues.
For one default tenant, give it the whole waiting allowance by setting both queue
limits to the same value. Construction performs no backend I/O and starts no workers.

### Namespace, Resource and Tenant

These names describe three different scopes:

| Scope | Purpose | Where to set it |
| --- | --- | --- |
| namespace | Separate application/environment names within a backend | The `namespace` argument of `postgres.New` or `redis.New` |
| Resource | An independent capacity pool with its own configuration, queues and leases | `SharedOptions.Resource` |
| Tenant | A contender sharing one Resource, used for tenant FIFO, round-robin scheduling and per-tenant queue limits | `AcquireFor` or `AcquireNFor`; `Acquire` uses the default tenant |

```text
namespace: order-service
├── Resource: export-jobs (capacity 4)
│   ├── Tenant: customer-A ─┐
│   └── Tenant: customer-B ─┴─ share these 4 tokens
└── Resource: image-jobs (capacity 8)
    ├── Tenant: customer-A ─┐
    └── Tenant: customer-B ─┴─ share these 8 tokens
```

Namespace isolates names; it does not impose an aggregate capacity limit or an
access-control boundary. Resource isolates capacity and scheduling state. Tenant
provides fairness within that shared capacity, not a separate capacity pool or a
hard running-concurrency quota. Using one Resource per customer would give each
customer independent capacity instead of enforcing a common total limit.

PostgreSQL shares tables: resource metadata has primary key `(namespace, resource)`,
and requests have primary key `(namespace, resource, id)`. Commands lock only the
matching resource row with `FOR UPDATE` and read that resource's requests. Redis
stores each resource at `airlock:{SHA256(namespace + "\0" + resource)}:state`, with
independent snapshot CAS. Resources still share the backend's physical resources
and connection pool; logical isolation does not guarantee performance isolation.

Separate Driver objects do not imply separate capacity. Clients using the same
PostgreSQL database/table/namespace/resource, or the same Redis keyspace/namespace/resource,
share capacity and must use the same resolved `ResourceConfig`. Use different
Resources for independent pools; create separate drivers when different namespaces,
connections or PostgreSQL tables are needed. Memory has no namespace argument:
its sharing scope is the driver instance, with separate state per Resource.

### Reuse one driver for multiple independent Resources

First choose one backend and set its namespace explicitly. For PostgreSQL,
`db` is your application-owned `*sql.DB`; `postgres` is
`github.com/tr1v3r/airlock/driver/postgres`. Create the schema during deployment.

```go
namespace := "order-service"
driver, err := postgres.New(db, namespace)
if err != nil {
    return err
}
```

For Redis, use this alternative initialization. `client` is your configured
`go-redis` client (with `ContextTimeoutEnabled: true`); `redisdriver` is an alias
for `github.com/tr1v3r/airlock/driver/redis`.

```go
namespace := "order-service"
driver, err := redisdriver.New(client, namespace)
if err != nil {
    return err
}
```

Then reuse the chosen driver for both Resources below. Both pools belong to
namespace `order-service`; `customer-A` identifies a tenant within each pool.

```go
newPool := func(resource string, capacity int) (*airlock.Shared, error) {
    return airlock.NewShared(driver, airlock.SharedOptions{
        Resource: resource,
        Config: airlock.ResourceConfig{
            Admission: airlock.Config{
                Capacity: capacity, MaxQueued: 100, MaxQueuedPerTenant: 20,
            },
        },
    })
}
exports, err := newPool("export-jobs", 4)
if err != nil {
    return err
}
images, err := newPool("image-jobs", 8)
if err != nil {
    return err
}
// Use exports.AcquireFor(ctx, "customer-A") for export work,
// and images.AcquireFor(ctx, "customer-A") for image work.
```

The pools above have independent limits of 4 and 8 tokens, even for the same tenant
name. A full export pool does not consume image capacity. Each pool can have its
own `ResourceConfig`; clients of the same pool must agree on that configuration.
Acquire starts a lease: release it after work stops, and supervise expiry/renewal
as described in section 3.

The executable [multi-resource example](../../driver/memory/example_test.go) uses
one memory driver and holds all 4 export tokens while acquiring all 8 image tokens.
It needs no server and demonstrates Resource isolation only: memory has no namespace
parameter. To exercise the namespace as well, replace `memory.New()` with one of
the PostgreSQL or Redis initializations above and provide the corresponding backend.

```sh
cd driver/memory
go test -run '^ExampleNew_multipleResources$' -v
```

## 2. Acquire using the same method names

| Task | Local and Shared call |
| --- | --- |
| One token, default tenant | `Acquire(ctx)` |
| One token, named tenant | `AcquireFor(ctx, tenant)` |
| Multiple tokens, default tenant | `AcquireN(ctx, n)` |
| Multiple tokens, named tenant | `AcquireNFor(ctx, tenant, n)` |

Shared returns `*Lease`; local returns `*Permit`. With waiting enabled, the call
waits until admission, rejection, cancellation or the waiting deadline. You do not
write a polling loop. `TryAcquire` currently exists only on local `Airlock`.

For a complete hello example with no external server, run from a repository
checkout containing this API:

```sh
cd driver/memory
go run ./examples/hello
```

This uses memory only to demonstrate `NewShared`; each separate memory driver has
separate capacity, and restarting it loses all state. Existing released versions
may not yet contain this development API.

## 3. Own the task and its lease lifecycle

A Permit lives until you release it. A Lease also has an expiry time. Calling
`Acquire` with a context only bounds acquisition: cancelling that context does
not revoke a lease already returned. If the same request context should also stop
the task, explicitly pass it into your task supervision code.

The [runnable lifecycle example](../../driver/memory/examples/lifecycle/main.go)
contains application-owned supervision and fault-ordering tests:

```sh
cd driver/memory
go run ./examples/lifecycle
go test -race ./examples/lifecycle
```

It demonstrates this sequence:

1. Acquire a lease; only then start work.
2. Give work a cancellable context. Watch `lease.Done()` independently of renewal
   I/O, so a blocked renewal cannot delay the cancellation signal for expiry.
3. Explicitly renew while work runs. Any renewal error stops work; no business
   retry is performed. Keep renewal frequency and I/O timeout well inside the TTL.
4. Wait for work, including all child tasks it started, to stop. Then stop and
   join the renewal/watcher goroutines.
5. Release using a fresh bounded context independent of the cancelled work
   context. If release fails, its outcome may be unknown. This example retries
   cleanup once on the **same Lease**; it never reruns the task. If both attempts
   fail, return the cleanup error along with any work error.

The demo shortens TTL to 1s, renews every 250ms, and uses 100ms operation contexts
so renewals are visible in a short memory-only run. These are demo timings, not
remote-backend tuning recommendations. With the ordinary 30s TTL, an application
might renew every 10s with a 5s I/O bound, then tune for its actual delays.
No timing choice can survive an arbitrary process pause or partition.

The SDK does not execute tasks, automatically renew leases, or kill work that
ignores cancellation. The example's `runJob` helper is application code, not a
new SDK method. Lease expiry reclaims ledger capacity; it cannot stop stale
external writes. Such writes need application-specific downstream protection.

## 4. Adjust advanced settings only when needed

For the protocol and polling settings below, **zero** selects a default in
`NewShared`; explicit nonzero values remain unchanged, and invalid values still
fail validation. `ClockSkewAllowance` is opt-in: zero reserves no skew budget.
No defaults apply to direct Driver requests. The deprecated `NewDistributed` constructor forwards to the same
implementation and uses the same defaults.

| Setting | Default | Meaning |
| --- | --- | --- |
| `Config.LeaseTTL` | 30s | Lifetime of each grant or successful renewal |
| `Config.MaxWait` | 30s | Maximum queued lifetime; an earlier caller deadline wins |
| `Config.SubmitWindow` | 5s | Window in which the initial request can be submitted |
| `Config.Retention` | 1m | Recovery retention after reaching a terminal state |
| `Config.MaxRecords` | 10000 | Bound on active, queued and retained terminal records together |
| `OperationTimeout` | 5s | Maximum duration of each client-side backend call |
| `PollInterval` | 50ms | Delay between queries while queued |
| `ClockSkewAllowance` | 0 | Opt-in budget for a known client/backend clock-offset bound |

Built-in memory, PostgreSQL and Redis drivers use a read-only Observe fast path
until lease/queue expiry or history collection is due. Fast observations keep the
committed Revision; due observations run the ordinary atomic transition. Waiters'
own polling can advance maintenance without a holder calling Release.
Every queued acquisition still polls at PollInterval; there is no notification
transport or background maintenance worker.


Terminal records remain until both the submission window and retention requirement
permit removal. A full history can return `ErrHistoryFull` even when token capacity
is available. MaxRecords is an on-demand record bound, not preallocated capacity;
large histories increase snapshot processing cost. These defaults are a starting
policy, not a throughput or latency guarantee. A driver's own shorter operation
bound or the caller's earlier deadline can still take precedence.

All participants of the same resource must use the same **resolved ResourceConfig**.
Defaults are filled before requests reach the backend. Existing resources retain
their configuration: upgrading and omitting previously explicit, different values
will yield `ErrConfigConflict`, not an online policy migration. Keep the existing
explicit policy when upgrading, or introduce a distinct resource with a planned
cutover. OperationTimeout, PollInterval and ClockSkewAllowance are client-local settings.

### Reserve an explicit clock-skew budget for remote clients

Without an allowance, a client clock ahead of the backend by more than the time
spent delivering the command can repeatedly receive `ErrInvalidRequest`: its
absolute submission/waiting deadlines exceed the backend's maximum windows.
Increasing those windows alone does not remove the offset. Choose an allowance
from a **known deployment clock-offset bound**, not a universal recommended value:

```go
options.ClockSkewAllowance = 200 * time.Millisecond // Only if this bounds your deployment's skew.
```

Let the allowance be `A`. Zero keeps the previous construction behavior exactly.
A positive allowance must be whole milliseconds and satisfy both
`2*A < Config.SubmitWindow` and `2*A < Config.MaxWait` after defaults resolve;
otherwise construction returns `ErrInvalidConfig`. The client constructs:

```text
SubmitUntil = floor_ms(clientNow + SubmitWindow - A)
WaitUntil   = floor_ms(min(clientNow + MaxWait, callerDeadline) - A)
```

The caller deadline term is omitted when absent. This also shortens an earlier
caller deadline; it never extends how long the caller may wait. If a positive
allowance consumes the entire local waiting budget, `Acquire` returns
`ErrLeaseLost` without backend I/O. This does not mean `ctx` itself has expired;
an already cancelled context still returns its own error. For clocks within
`±A`, the backend's effective windows can be as short as `window - 2*A`, before
network/processing time is deducted. A short caller deadline may still expire at
the backend even if the local preflight succeeds.

The setting is not clock synchronization or a skew detector. With insufficient
budget, the backend can still reject or expire a request; errors never authorize
work and no fresh identity is automatically retried. The request ID remains bound
to its adjusted submission deadline, and backend validation, tombstones and GC are
unchanged. Lease Remaining/RTT accounting is unchanged: this budget does not add
lease time, handle arbitrary clock jumps or network delay, or provide fencing.
See [ADR 009](../decisions/009-clock-skew.md).

For protocol internals, use the separate [custom driver guide](drivers.md).
For the choices behind these defaults, see [ADR 008](../decisions/008-shared-usability.md).

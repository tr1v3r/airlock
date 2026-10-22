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

Only **zero** selects a default in `NewShared`; explicit nonzero values remain
unchanged, and invalid values still fail validation. No defaults apply to direct
Driver requests. The deprecated `NewDistributed` constructor forwards to the same
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
cutover. OperationTimeout and PollInterval are client-local settings.

For protocol internals, use the separate [custom driver guide](drivers.md).
For the choices behind these defaults, see [ADR 008](../decisions/008-shared-usability.md).

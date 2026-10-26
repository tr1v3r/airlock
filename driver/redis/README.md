# Airlock Redis driver

[中文](README.zh-CN.md)

Independent module `github.com/tr1v3r/airlock/driver/redis`. The core module keeps
standard-library-only runtime dependencies; this adapter uses `go-redis/v9`.
Miniredis is a test dependency. Requires Redis with Lua scripting and `TIME`.

Start with the [Shared user guide](../../docs/guides/shared.md): resource, capacity
and queue limits are enough to configure admission; advanced protocol settings
have defaults. It includes a complete explicit-renewal, task-cancellation and
release-cleanup example. Backend implementation details below are for deployment
and adapter maintenance.


```go
client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379", ContextTimeoutEnabled: true,
})
defer client.Close()
namespace := "my-service"
driver, err := redisdriver.New(client, namespace)
// Handle err, then pass driver to airlock.NewShared.
```

See [the runnable example](examples/basic/main.go):

```sh
AIRLOCK_REDIS_ADDR=127.0.0.1:6379 go run ./examples/basic
```

`New` performs no I/O, accepts a `*redis.Client` or `*redis.ClusterClient`, and
never closes or changes the caller's client. `ContextTimeoutEnabled` must be true;
Cluster read-only/replica routing is rejected. Socket deadline updates must remain
enabled: raw `ReadTimeout`/`WriteTimeout=-2` (and other disabling negative values)
are rejected after go-redis normalizes the options. Raw `-1` is accepted: it
disables the configured socket timeout but still applies context deadlines.
`WithOperationTimeout` bounds the
whole operation (default five seconds); earlier caller deadlines prevail.
Do not mutate client options or install hooks that retry ambiguous operations.

## Atomicity, time, and retries

Each resource uses one persistent key:
`airlock:{sha256(namespace + NUL + resource)}:state`. Namespace is valid UTF-8,
without NUL, at most 256 bytes. The hash tag gives every operation one Cluster
slot. Missing and existing-empty keys are distinguished: empty, corrupt, and
wrong-type state fails closed. Keys never expire and retain config/cursor even
when idle. All participants must use the same immutable resource configuration.

A Lua read atomically obtains state and Redis `TIME`. Go computes the shared
state transition. Lua then compares the exact old value and checks the commit
time window before a single `SET`. The window includes live lease expiry,
waiting/submission deadlines, and history cleanup boundaries, so a lease cannot
be renewed from an expired read. Server time is preserved to microseconds and
checked against Lua's exact integer range. All validation precedes `SET`; no
potentially failing command follows it. Redis scripts do not roll back writes.

Only explicit comparison/time-window misses trigger bounded, context-aware
backoff and a fresh computation. Each script command disables go-redis network
retries using `Cmder.NoRetry`; a definite `NOSCRIPT` permits `EVAL` fallback.
Cluster's explicit `MOVED`/`ASK` routing still works. Transport or script errors
return a zero result: the write may already have succeeded. In particular, an
uncertain renewal is not repeated inside the adapter. Business rejections still
commit expiry and scheduling changes. No connection is held during business work.

The whole bounded snapshot is read, encoded, and written for each operation.
This keeps the memory/PostgreSQL/Redis scheduling contract identical but costs
O(records) transfer/work and CAS retries on hot resources. There is no global
lock across resources, no Pub/Sub wakeup, no background cleanup, and no automatic
renewal. The core distributed client uses bounded polling; arrival at the backend
sets admission order, and polling frequency does not select grants.

## Operational limits

Use a dedicated, access-controlled Redis deployment with **no eviction** and no
external state writers/TTL/delete/restore during active admission. Capacity
safety assumes authoritative state is not lost. Redis asynchronous replication
and failover can lose acknowledged state; persistence and replicas alone do not
turn this adapter into a linearizable distributed lock. It supplies no external
fencing token. Losing a key while an old holder continues working can over-admit.
For workloads requiring that failure guarantee, select a suitably configured
transactional backend and enforce external fencing where needed.

Callers must stop actual work when `Lease.Done` closes or `Valid` is false. A
lease expiry only recovers accounting; it cannot stop CPU work or undo writes.
Renew explicitly, stop work before release, and handle business retries outside
the limiter. An acquisition context only bounds waiting, not the delivered lease.
Unknown renewals invalidate the local lease. Retry cleanup with the same lease;
never replace an ambiguous request with a new identity to assume capacity.

Resource keys are permanent. Administrative removal or capacity changes require
quiescing all clients and active work. Retention/MaxRecords bound per-resource
recovery history, not the total number of resource keys. Grant/queue weights,
priority selection, protected heads, and terminal idempotency use the core driver
contract. Priority turns count requests, not resource time, and provide no latency
SLA. A high-priority resource can be isolated with a separate key and fixed budget.

## Verification

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
# Use isolated servers: integration tests flush the script cache.
AIRLOCK_REDIS_ADDR=127.0.0.1:6379 GOWORK=off go test -race -run TestIntegrationStandalone ./...
AIRLOCK_REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 GOWORK=off go test -race -run TestIntegrationCluster ./...
```

For container Cluster fixtures whose announced addresses are not host-routable,
set `AIRLOCK_REDIS_CLUSTER_MAPPING=containerIP:6379=127.0.0.1:port,...`. This is a
test-only dial mapping. Tests exercise the shared conformance suite, CAS races,
expiry guards, cache reload, lost write responses, and client lease lifecycle.
Default tests use miniredis; real standalone and Cluster tests are opt-in.

The public operations are `Submit`, `Observe`, `Cancel`, `Renew`, and `Release`.
The first three accept an immutable `protocol.Request`; renewal and release use
`protocol.LeaseRef{Request: original, ID: result.LeaseID}`. All return `(protocol.Result, error)`.

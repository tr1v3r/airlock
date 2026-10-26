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
slot. The key is a hash: `state` holds the authoritative JSON snapshot, `meta`
holds observation metadata, and `r:<request-id>` holds each indexed request.
Missing keys and hashes missing their snapshot are distinguished; empty, corrupt,
and wrong-type state fails closed. Keys never expire and retain config/cursor even
when idle. All participants must use the same immutable resource configuration.

For mutations and due maintenance, a Lua read atomically obtains the snapshot
and Redis `TIME`; Go computes the shared transition. Lua compares the exact old
snapshot and checks the commit window, including lease expiry, waiting/submission
deadlines and history cleanup. Time retains microsecond precision within Lua's
exact integer range.

The script prepares request indexes tagged with a unique candidate token, then
publishes the snapshot and matching observation header together in its final
`HSET`. There is no command after that publication. A preparation error cannot
authorize work: missing or token-mismatched projections fall back to the
authoritative snapshot. Tokens are independent of Revision so different failed
candidates for the same revision cannot be mistaken for a committed result.
Redis scripts do not roll back earlier writes.

Only explicit comparison/time-window misses trigger bounded, context-aware
backoff and a fresh computation. Each script command disables go-redis network
retries using `Cmder.NoRetry`; a definite `NOSCRIPT` permits `EVAL` fallback.
Cluster's explicit `MOVED`/`ASK` routing still works. Transport or script errors
return a zero result: the write may already have succeeded. In particular, an
uncertain renewal is not repeated inside the adapter. Business rejections still
commit expiry and scheduling changes. No connection is held during business work.

An ordinary `Observe` atomically reads only `meta`, the selected request and
backend time. Before the next lease/queue expiry or history collection boundary,
it returns the committed result without reading the full snapshot, restoring the
scheduler, changing Revision or executing CAS. Missing projections or due
maintenance fall back to the ordinary transition; confirmed CAS losers recheck
the projection before recomputing.

Mutations still read/write the bounded snapshot and maintain the request index,
with O(records) work and additional memory. No Pub/Sub or worker is introduced.
The core client's default polling interval remains 50ms per queued acquisition;
this change reduces each poll's cost, not the number of polling callers.
Independent resources use different keys but share backend CPU/network/pool
capacity. Arrival at the backend determines queue order, not polling frequency.

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
The first three accept an immutable `airlock.Request`; renewal and release use
`airlock.LeaseRef{Request: original, ID: result.LeaseID}`. All return `(airlock.Result, error)`.

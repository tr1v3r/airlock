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
`WithOperationTimeout` bounds the operation (default five seconds), including update-lock contention; earlier caller
deadlines prevail. Best-effort lock cleanup may add at most 100ms after cancellation
or timeout. `WithUpdateLockTTL` sets the update lease (default one second), rounded
up to whole milliseconds; it does not change the application lease TTL.
Do not mutate client options or install hooks that retry ambiguous operations.

## Atomicity, time, and retries

Each resource uses one persistent key:
`airlock:{sha256(namespace + NUL + resource)}:state`. Namespace is valid UTF-8,
without NUL, at most 256 bytes. The hash tag gives every operation one Cluster
slot. The key is a hash: `state` holds the authoritative JSON snapshot, `meta`
holds observation metadata, and `r:<request-id>` holds each indexed request.
Missing keys and hashes missing their snapshot are distinguished; empty, corrupt,
and wrong-type state fails closed. State keys never expire and retain config/cursor
even when idle; the separate update-lock keys described below have a TTL. All
participants must use the same immutable queue/lease policy; creation-capacity hints may differ.

Mutations and due maintenance first contend for
`airlock:{same-resource-hash}:update-lock`, a separate string key in the same
Cluster slot. Like an owner-checked lease, acquisition uses `SET NX PX` with a
fresh random owner token. A loser backs off without loading or computing the
full snapshot. `Submit`, `Cancel`, `Renew` and `Release` all use this path.

With warm connections and script caches, a contention-free operation that publishes
state without retry takes four Redis round trips: acquire lock → read snapshot →
commit → release lock. A read-only Observe takes one; Observe performing full
maintenance normally takes six, including the initial and under-lock projection
reads. Script loading, redirects and retries add requests. The update lock reduces
duplicate computation under contention while adding round trips to uncontended writes.

After acquisition, Lua reads the snapshot and Redis `TIME`; Go computes the shared
transition. The commit script checks **current lock ownership and CommitWindow
together**, including lease expiry, waiting/submission deadlines and history
cleanup. Time retains microsecond precision within Lua's exact integer range.
All state writers must acquire the same resource lock. The commit sends only
owner, new state, window bounds and observation index; it neither sends nor
compares the previous snapshot. Time guards remain necessary because holding a
lock does not stop lease and queue deadlines from passing. A paused updater whose
lock expires cannot publish even if the snapshot has not changed.

After each acquired attempt, a separate owner-checking Lua script deletes only
that updater's lock, including on cancellation or an error. Cleanup uses a fresh,
bounded context, with a budget of `min(operation timeout, 100ms)`. Return can
therefore follow the caller deadline or operation timeout by up to that cleanup
budget. Unlock errors neither roll back committed state nor turn an uncertain
command into success. The operation's final context check still applies, and an
unremoved lock expires via TTL. An uncertain acquisition returns an error without replay or state work;
its possibly installed lock also expires. No lock-renewal goroutine runs: choose
an update TTL long enough for the bounded snapshot computation and commit. A
confirmed expiry discards the candidate, releases any remaining ownership and
retries from a fresh read within the operation deadline. The lock covers driver
state updates only and is never held while application work runs.

The script prepares request indexes tagged with a unique candidate token, then
publishes the snapshot and matching observation header together in its final
`HSET`. Within the commit script, no command follows that publication. A
preparation error cannot authorize work: missing or token-mismatched projections fall back to the
authoritative snapshot. Tokens are independent of Revision so different failed
candidates for the same revision cannot be mistaken for a committed result.
Redis scripts do not roll back earlier writes.

Only known lock contention or explicit ownership/time-window misses
trigger bounded, context-aware backoff (1–10ms). Each script command disables
go-redis network retries using `Cmder.NoRetry`; a definite `NOSCRIPT` permits `EVAL` fallback.
Cluster's explicit `MOVED`/`ASK` routing still works. Transport or script errors
return a zero result: the write may already have succeeded. In particular, an
uncertain renewal is not repeated inside the adapter. Business rejections still
commit expiry and scheduling changes. No connection is held during business work.

An ordinary `Observe` atomically reads only `meta`, the selected request and
backend time. Before the next lease/queue expiry or history collection boundary,
it returns the committed result without reading the full snapshot, restoring the
scheduler, acquiring the update lock, changing Revision or publishing state. Missing
projections or due maintenance attempt the update lock. Observe rechecks the
projection after acquiring ownership: if another updater already performed
maintenance, it releases the lock and returns the committed result without full
state work. Contenders also recheck the projection between attempts, so they can
return a new result without acquiring the lock themselves.

Mutations still read/write the bounded snapshot and maintain the request index,
with O(records) work and additional memory. No Pub/Sub or worker is introduced.
The core client's default polling interval remains 50ms per queued acquisition;
this change reduces each poll's cost, not the number of polling callers.
Independent resources use different state and update-lock keys but share backend
CPU/network/pool capacity. Queue admission order follows successful state
transitions; the update lock does not promise FIFO ownership. Call start order
and polling frequency do not guarantee grant order.

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

Creation capacity and request weight use positive Go `int` values; Resize targets
and current capacity may be zero, up to `2^63-1` on
64-bit clients or `2^31-1` on 32-bit clients. The snapshot and embedded projection
JSON retain exact integer text; Lua does not convert capacity or weight to a
floating-point number. All participants must support the chosen integer range.
Decoding unrepresentable values or summing overflowing charged weights fails
closed. MaxRecords and queue-count limits remain unchanged. The Lua exact-time
bound applies to backend timestamps, independently of capacity.

State keys are permanent; update-lock keys expire. Administrative removal requires quiescing all clients and active work. Online capacity changes use ResourceManager.Resize. Retention/MaxRecords bound per-resource
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
test-only dial mapping. Tests exercise the shared conformance suite, competing updaters,
expiry guards, lock contention/expiry/owner checks, read-only polling under an
update lock, cache reload, lost write responses, and client lease lifecycle.
Default tests use miniredis; real standalone and Cluster tests are opt-in.

The public operations are `Submit`, `Observe`, `Cancel`, `Renew`, and `Release`.
The first three accept an immutable `airlock.Request`; renewal and release use
`airlock.LeaseRef{Request: original, ID: result.LeaseID}`. All return `(airlock.Result, error)`.

Resize accepts nonnegative targets; negative targets return `ErrInvalidConfig`.
Zero pauses new grants, retaining waiters, fair order and the protected head.
New positive-weight blocking acquires queue under normal budgets/deadlines;
existing leases may renew, release and expire normally. Resuming positive capacity
rejects queued weights above the target before fair scheduling, including expansion
from zero. Creation still requires positive capacity; zero is not unlimited mode.
Capacity-version CAS and same-capacity maintenance rules are unchanged.

Resource management, capacity-version conflicts and read-only Inspect semantics are documented in the [Shared guide](../../docs/guides/shared.md#shared-dynamic-capacity).

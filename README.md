# Airlock

[English](README.md) | [简体中文](README.zh-CN.md)

A Go 1.24+ library for weighted, tenant-aware concurrency admission.
It provides FIFO waiting within each tenant, round-robin selection across waiting
tenants, optional priority groups with weighted selection and reserved waiting
budgets, and global/per-tenant queue limits. Runtime dependencies are standard
library only. Callers own goroutines, results, errors, and completion.

## Installation

```sh
go get github.com/tr1v3r/airlock
```

This API is under development on the feature branch targeting `dev`; it is not
available in a tagged release yet.

## Hello world

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/tr1v3r/airlock"
)

func main() {
	lock, err := airlock.New(airlock.Config{Capacity: 2})
	if err != nil {
		log.Fatal(err)
	}
	permit, err := lock.Acquire(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer permit.Release()
	fmt.Println("hello, airlock")
}
```

`Acquire(ctx)` uses one shared default tenant. With only `Capacity` configured,
full capacity returns `ErrQueueFull` immediately. To wait, set **both** queue
limits, for example `Config{Capacity: 2, MaxQueued: 8, MaxQueuedPerTenant: 8}`.
The default tenant uses the same queue limits, FIFO and cancellation rules as
named tenants; it does not collide with any explicit tenant ID.

## Immediate acquisition

Use `TryAcquire` to decide per call whether to wait on the **same** Airlock:
`Acquire` calls may join its bounded queues, while `TryAcquire` calls never queue
or wait for capacity. All methods return `(*Permit, error)`:

| Default tenant | Named tenant | Tokens |
| --- | --- | --- |
| `TryAcquire()` | `TryAcquireFor(tenantID)` | 1 |
| `TryAcquireN(n)` | `TryAcquireNFor(tenantID, n)` | n |

```go
permit, err := lock.TryAcquireNFor("online", 2)
if errors.Is(err, airlock.ErrWouldBlock) {
    // Reject or fall back without waiting for capacity.
    return nil
}
if err != nil {
    return err
}
defer permit.Release()
// Execute work.
```

This snippet belongs in an error-returning function and uses the `errors` package.
See the runnable [immediate acquisition example](examples/tryacquire/example.go).

- A successful call atomically acquires the full weight and uses the same
  `Permit.Release()` as `Acquire`. It requires enough capacity **and no waiting
  requests**; it cannot bypass a protected large request even if smaller work fits.
- Otherwise it returns `ErrWouldBlock` with no permit and no changes to capacity,
  queues, or tenant rotation. Queue allowances do not affect this decision.
  `ErrWouldBlock` means immediate admission is unavailable; `ErrQueueFull`, `ErrGroupQueueFull`, and
  `ErrTenantQueueFull` instead describe rejected attempts to join an `Acquire` queue.
- Explicit tenant IDs are validated before weights. Empty IDs return
  `ErrInvalidTenant`; nonpositive weights return `ErrInvalidWeight`, and weights
  above capacity return `ErrWeightExceedsCapacity`. Use `errors.Is` to distinguish
  these invalid requests from temporary unavailability.
- These local calls take no context. They still acquire the internal mutex;
  “immediate” means no capacity wait, not a wait-free or bounded-latency guarantee.
  Do not loop over `TryAcquire` to implement waiting: it creates contention and
  obtains no place in the fair queue. Use `Acquire` with queue limits and a context.

The [TryAcquire design decision](docs/decisions/002-try-acquire.md) records the
fairness boundary and API alternatives (document in Chinese).

## Local priority groups

Priority is configured once at construction; existing acquisition signatures stay
unchanged. For example, use more selection turns for online tenants while keeping
background tenants eligible under sustained load:

```go
lock, err := airlock.New(airlock.Config{
    Capacity: 20, MaxQueued: 120, MaxQueuedPerTenant: 10,
    Priority: &airlock.PriorityPolicy{
        Groups: []airlock.PriorityGroup{
            {Name: "high", Turns: 8, MaxQueued: 80},
            {Name: "normal", Turns: 3, MaxQueued: 30},
            {Name: "low", Turns: 1, MaxQueued: 10},
        },
        DefaultGroup: "low",
        TenantGroups: map[string]string{
            "online": "high", "batch": "normal",
        },
    },
})
if err != nil {
    return err
}
permit, err := lock.AcquireNFor(ctx, "online", 3)
if err != nil {
    return err
}
defer permit.Release()
```

This snippet belongs in an error-returning function. The runnable
[priority example](examples/priority/example.go) queues low-priority work first,
then demonstrates the `8:3:1` selection order without depending on goroutine start order.

- `Priority: nil` keeps the existing single-group behavior. With a policy, the
  default tenant and unmapped named tenants use `DefaultGroup`. `New` deep-copies
  the groups and tenant mapping; later caller changes have no effect. There is no
  live remapping. Configure mappings from trusted identities: this is scheduling,
  not authentication, and callers must not be allowed to impersonate a high-group tenant.
- Groups must be nonempty, with unique nonempty names, positive `Turns` in
  non-increasing order, and total turns at most 1024. The default and all mapped
  groups must exist; mapping keys cannot be empty. Group queue limits are
  nonnegative and their sum cannot exceed `MaxQueued`. Violations return
  `ErrInvalidConfig`.
- `8:3:1` means the repeating selection slots `HHHHHHHHMMML`. Within each group,
  tenants rotate and requests remain FIFO. Empty groups are skipped, with no
  accumulated credit. A selected request consumes its slot even if later cancelled.
  Immediate acquisitions when nobody is waiting consume no slot.
- Shares count **request selections**, not tokens or execution time. With every
  group continuously backlogged and no selected-request cancellations, a full
  cycle grants eight high, three normal, and one low request. This is not strict
  priority or preemption. An already-selected low-group large request blocks later
  high-group work until it fits or is cancelled; `TryAcquire` cannot bypass it.
- Group queue allowances are reserved and cannot be borrowed. Unassigned global
  allowance is not shared automatically. Queue rejection order is global
  `ErrQueueFull`, then group `ErrGroupQueueFull`, then tenant `ErrTenantQueueFull`.
  Queue limits count requests, including a protected request; zero group allowance
  disables queuing for that group but still permits immediate acquisition.
- Positive turns preserve progress for accepted, uncancelled requests if holders
  eventually release and scheduling progresses. A continuously waiting low group
  sees at most 11 other selections between its turns in `8:3:1`; this is neither a
  per-tenant bound nor a time bound. Queue admission itself may still be rejected.
  `Stats` remains an aggregate snapshot; per-group statistics are not exposed yet.

See the [priority design tradeoffs](docs/decisions/003-local-priority.md) and
[comparison results](docs/proposals/002-priority-experiment-results.md) for the
fairness boundaries and utilization cost (documents in Chinese).

## Isolating latency-sensitive work

Weighted selection does not guarantee high-priority admission latency. For a
fixed resource budget, compose independent instances: for example, reserve 4 of
12 tokens for high-priority work and use the remaining 8 for normal/low groups.
Their queues and protected heads are independent. Route using trusted identity;
do not borrow reserved capacity or add a shared outer blocking gate.

The [isolation example](examples/isolation/example.go) runs online work while all
low-pool tokens remain held, and separates acquisition and execution deadlines.
Queue timeout does not retry; cancellation after acquisition does not revoke a
permit. Release only after the task actually stops using the resource.

The tradeoffs are idle reserved capacity and earlier rejection when the high pool
itself overloads. Request weights must fit their destination pool; replacing pools
while old permits remain live can exceed the total budget. This does not isolate
CPU, downstream services, or a shared connection pool, and is not a latency SLA.
The [proposal](docs/proposals/003-high-priority-isolation.md) and
[measurements](docs/proposals/003-isolation-experiment-results.md) (Chinese)
evaluate composition without introducing a partitioning API.
See the [isolation tradeoffs](docs/decisions/004-high-priority-isolation.md) for
reservation costs, timeout/retry ownership, weight limits and migration boundaries.

## Usage and contract

```go
lock, err := airlock.New(airlock.Config{
    Capacity:           20,
    MaxQueued:          100,
    MaxQueuedPerTenant: 10,
})
if err != nil {
    return err
}
permit, err := lock.AcquireFor(ctx, tenantID)
if err != nil {
    return err
}
defer permit.Release()
// Execute work, passing ctx to it separately.
```

This snippet belongs in an error-returning function. See the runnable
[tenant example](examples/tenants/example.go). For weighted work, replace the
acquisition with `lock.AcquireNFor(ctx, tenantID, 3)` to reserve three tokens;
the same `permit.Release()` returns all three. Without a tenant ID, use
`lock.AcquireN(ctx, 3)`. See the [weighted example](examples/weighted/example.go).

- Capacity must be positive. Queue limits must be nonnegative, and the per-tenant
  limit cannot exceed the global limit; otherwise `New` returns
  `ErrInvalidConfig`. Zero queue limits disable waiting. Limits are fixed.
- `Acquire(ctx)` and `AcquireFor(ctx, tenantID)` acquire one token. Use
  `AcquireN(ctx, n)` for the default tenant or `AcquireNFor(ctx, tenantID, n)`
  for a named tenant to acquire all `n` tokens atomically; requests never hold
  partial capacity while waiting. Explicit tenant IDs must be nonempty strings.
  Use trusted, bounded IDs: Airlock does not authenticate callers or bound lengths.
- For `Acquire` methods, a nil context panics. Explicit tenant ID validation comes next
  (`ErrInvalidTenant`), then weight validation: `n <= 0` returns
  `ErrInvalidWeight`; `n > Capacity` returns `ErrWeightExceedsCapacity`.
  Invalid weights fail immediately without queuing, even with a cancelled context.
- If immediate acquisition is not possible, full queues return `ErrQueueFull`,
  `ErrGroupQueueFull` (with priority enabled), or `ErrTenantQueueFull`, checked in that order.
  An already-cancelled context returns its error before available-capacity and queue checks.
  Errors are compatible with `errors.Is`; failed acquisitions return no permit.
- Nonempty tenant queues take turns within their group, with FIFO order inside each queue. When
  capacity is available, the scheduler selects the next tenant's head request
  and consumes that tenant's turn. If its full weight does not fit, it remains
  protected: smaller or newly arriving requests cannot bypass it. Cancelling
  this selected request does not restore its turn. No request is preselected
  while capacity is completely full.
- New waiting tenants join the end of the rotation. An empty tenant queue is
  removed; when it becomes active again it rejoins at the end. Fairness counts
  selection opportunities, not token counts or execution time. Protecting a
  large request can leave tokens idle and delay other tenants; it prevents
  endless bypass only if existing holders eventually release and scheduling
  continues.
- Queue limits count pending requests, not weights. One tenant may use all
  token capacity when others are idle. Running work is never preempted.
  Fairness does not guarantee equal execution time or bounded latency, or protect a
  new tenant from rejection when the global waiting queue is already full.
- Cancellation and granting are serialized. If cancellation wins, no token is
  retained. If granting wins, Acquire succeeds even if cancellation follows:
  release every successful permit. Cancellation does not stop admitted work.
- Use the returned `*Permit` without copying it. `Release` is concurrency-safe
  and idempotent, returning the entire acquired weight exactly once; nil/zero
  permits do nothing. Partial release and permit resizing are not supported.
  This is an in-process permit, without a lease or automatic expiry/release.
- `Stats()` returns one consistent `Stats` snapshot: `Capacity`, `InUse`,
  `ActivePermits`, `Queued`, and `WaitingTenants`. `Capacity` and `InUse` count
  tokens, `ActivePermits` counts unreleased permits, and `Queued` counts waiting
  requests. With single-token calls, `InUse == ActivePermits`. This snapshot is
  not a completion barrier. Airlock must be constructed with `New` and must not
  be copied. It starts no goroutines.

The [local weighted admission tradeoffs](docs/decisions/001-local-weighted-tradeoffs.md)
record why this implementation protects large requests, uses request-based
fairness, and keeps explicit release, including costs and reassessment criteria
(document in Chinese).

The [initial design record](docs/proposals/001-tenant-fair-admission.md) describes
the single-token foundation. The [weighted contract](docs/proposals/002-weighted-scheduling.md)
extends it with the implemented local priority policy. [Historical prototype measurements](docs/proposals/001-experiment-results.md)
do not establish superiority over the strongest nested semaphore baseline or
production latency guarantees. The comparison workload now exercises the public
Airlock API; `x/sync` remains a test-only dependency.

```sh
GOWORK=off AIRLOCK_LOAD=1 go test -v ./internal/comparison -run '^TestLoadComparison$' -count=1
GOWORK=off go test ./internal/comparison -run '^$' -bench BenchmarkAdmission -benchmem -count=3
```

## Examples and validation

Run from `airlock/`:

| Example | Command |
| --- | --- |
| [Hello world](examples/hello/example.go) | `go run ./examples/hello` |
| [Bounded batch and ordered results](examples/batch/example.go) | `go run ./examples/batch` |
| [Acquisition timeout](examples/cancellation/example.go) | `go run ./examples/cancellation` |
| [Caller-owned completion notification](examples/completion/example.go) | `go run ./examples/completion` |
| [Tenant-aware single-token admission](examples/tenants/example.go) | `go run ./examples/tenants` |
| [Multi-tenant weighted admission](examples/weighted/example.go) | `go run ./examples/weighted` |
| [Immediate acquisition and fallback](examples/tryacquire/example.go) | `go run ./examples/tryacquire` |
| [Priority selection order](examples/priority/example.go) | `go run ./examples/priority` |
| [Reserved capacity for online work](examples/isolation/example.go) | `go run ./examples/isolation` |

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

## CI quality gates

Pull requests, pushes to `dev`/`main`, merge queues, manual runs, and a weekly
scan run [CI](.github/workflows/ci.yml). All jobs must pass the final **CI Gate**:

- `gofmt`, `go vet`, and pinned golangci-lint check correctness, error handling,
  exported API comments, naming, spelling, and explained lint suppressions.
  The quality job pins Go 1.27.0 with golangci-lint v2.13.2; upgrade and validate
  them together because analyzer support can lag Go export-format changes.
  Compatibility tests and vulnerability scans still track stable Go.
- Module tidiness/checksums and standard-library-only runtime dependencies are enforced.
- Tests run without cache and in shuffled order on Linux (Go 1.24, 1.25,
  previous stable, and stable), plus macOS and Windows (stable). `GOTOOLCHAIN=local`
  prevents automatic toolchain upgrades from hiding minimum-version failures.
  All documented examples execute on every matrix entry.
- Race detection, ten shuffled repetitions at CPU counts 1/2/4, and atomic
  statement coverage run on Linux. Every production file and the total must
  reach **95%** (current core baseline: **100%**). Examples and comparison-only
  code are excluded from coverage; new production packages are included automatically.
  Coverage profiles are retained for 14 days, including on failure.
- Pinned `govulncheck -test` rejects reachable known vulnerabilities in the SDK
  and tests. This does not certify the absence of all vulnerabilities.
- Workflow syntax and coverage-checker regression tests are checked. Actions
  use full commit pins, read-only permissions, job timeouts, and no PR secrets.
  Dependabot proposes weekly Action/module updates; review pinned CLI versions
  in the workflow separately.

Local equivalents (Go, Python 3, Bash, and the pinned tools are required):

```sh
bash scripts/check.sh format
bash scripts/check.sh modules
GOWORK=off go vet ./...
golangci-lint config verify && golangci-lint run
actionlint
python3 -m unittest discover -s scripts -p 'test_*.py'
GOWORK=off go test -shuffle=on -count=1 -timeout=5m ./...
bash scripts/check.sh coverage
GOWORK=off go test -race -shuffle=on -count=10 -cpu=1,2,4 -timeout=5m ./...
bash scripts/check.sh examples
GOWORK=off govulncheck -test ./...
```

`modules` runs `go mod tidy` and fails on manifest drift; it may update your
local manifests for review. Tool installation commands and versions are in the workflow.
Coverage is measured on executable statements, not branches or correctness;
do not lower the threshold or suppress findings simply to make CI green.
Performance comparisons remain opt-in because shared runners cannot provide a
stable regression baseline. API compatibility checks need an agreed released
API baseline and are not yet enforced.

**Repository setup:** after the workflow is pushed and runs, configure the
`dev`/`main` branch ruleset to require **CI Gate**, require pull requests and
review approval, and disallow bypasses/force pushes as appropriate. Workflow files
alone cannot prevent merging. This change does not alter remote repository rules.
Weekly schedules run from the repository's default branch.

## Migration

The local admission type is `Airlock`, constructed with `New(Config)`. Every
`Acquire(ctx)` or `AcquireFor(ctx, tenantID)` still acquires **one token**.
The new `AcquireN(ctx, n)` and `AcquireNFor(ctx, tenantID, n)` acquire `n` tokens,
returned together by `Permit.Release()`. `Config.Priority` optionally enables local
priority groups without changing these signatures. Unlimited mode and dynamic capacity are not implemented. Distributed leases and the memory driver use a separate API;
PostgreSQL and Redis have separate drivers. Existing keyed configurations with
`Priority: nil` (or omitted) retain single-group behavior. The new `Config.Priority`
field requires callers using unkeyed `Config` literals to migrate to keyed fields.

This is a breaking development change: `Pool`/`NewPool` and the earlier
`Gate`/`NewGate` names are removed. Existing callers must select explicit queue
limits, optionally supply a tenant ID, retain the returned permit, and release it.
`Stats().InUse` and `Stats().Capacity` replace `Num()` and `Size()` snapshots.
For a simple concurrency cap without tenant admission, use `x/sync/semaphore`.
No compatibility aliases are provided.

Use `Acquire(ctx)` for the default tenant and `AcquireFor(ctx, tenantID)` for
named tenants. Migrate former `Acquire(ctx, tenantID)` calls to `AcquireFor`;
interfaces, mocks, and method-value types must declare the corresponding fixed
signature. All acquisition entry points use fixed arguments. `Stats.ActivePermits`
adds a distinct permit count; update unkeyed `Stats` literals to keyed literals
when adopting the new field.

The original channel strategy is retained only as a test baseline. The initial
code migrated from `tr1v3r/pkg` commit
`b3e47bdfd27a149ecb4da01580b91be277b905f6` (`pools/`); Git history preserves it.
Use a caller-owned `sync.WaitGroup` to join tasks, including already-started tasks
when a later acquisition fails. Airlock does not execute jobs or track completion.

## Design proposal

The [weighted, priority-aware admission and optional drivers proposal](docs/proposals/002-weighted-priority-drivers.md) describes local, Redis, and PostgreSQL support. This branch implements local phases 1 and 2: atomic multi-token acquisition,
priority groups with tenant round-robin, reserved waiting budgets, and head-request
protection. The driver contract, lease client and memory reference are implemented;
PostgreSQL and Redis are provided as independent modules. This does not indicate a tagged release.

## Distributed leases and drivers

This branch implements the driver protocol, `NewShared`, and the independent
[memory](driver/memory), [PostgreSQL](driver/postgres), and [Redis](driver/redis) drivers. Local `New(Config)` and `Permit` are unchanged.
`NewShared(driver, SharedOptions)` returns a `*Shared` for admission coordinated
through the chosen driver. The driver determines the sharing scope. Construction
validates and copies configuration without backend I/O or acquiring tokens.
The previous `NewDistributed`, `Distributed`, and `DistributedOptions` names
remain deprecated compatibility aliases/entry points.

Use the independent [memory module](driver/memory) to exercise the same protocol
in one process. It does not provide durable storage or cross-process capacity.

The driver exposes `Submit(ctx, Request)`, `Observe(ctx, Request)`, and
`Cancel(ctx, Request)`, plus `Renew(ctx, LeaseRef)` and `Release(ctx, LeaseRef)`.
Each returns `(Result, error)`. `LeaseRef{Request: original, ID: result.LeaseID}`
identifies the grant. Custom drivers must implement these five methods; the
earlier `Execute(ctx, Command)` interface has been removed. `Command` remains
a state-machine helper for driver implementations.

`Shared.AcquireNFor(ctx, tenant, n)` returns `(*Lease, error)`. Waiting uses
bounded persistent queues and periodic Observe; a lease requires explicit
`Renew(ctx)` and `Release(ctx) error`. Stop work when `Done()` closes or `Valid()`
is false. The acquisition context does not revoke a delivered lease. There is
no automatic renewal or business retry. Network errors never authorize work.

See the [contract and tradeoffs](docs/decisions/005-driver-contract.md) and
[module guide](MODULES.md). Leases bound valid allocations, not physical work
that ignores expiry; they do not provide generic fencing or a latency SLA.

PostgreSQL uses caller-owned `*sql.DB`, explicit schema setup, per-resource row
locks and database time after locking. See its [tradeoffs and tested failover
model](docs/decisions/006-postgres-driver.md).

Redis uses a caller-owned go-redis client, one permanent key per resource, and
backend-time-guarded snapshot CAS. Async failover can lose acknowledged grants;
see the [Redis example and setup](driver/redis) and [tradeoffs and fault model](docs/decisions/007-redis-driver.md).

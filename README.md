# Airlock

[English](README.md) | [简体中文](README.zh-CN.md)

A Go 1.24+ library for single-token, tenant-aware concurrency admission.
It provides FIFO waiting within each tenant, round-robin admission across waiting
tenants, and global/per-tenant queue limits. Runtime dependencies are standard
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
permit, err := lock.Acquire(ctx, tenantID)
if err != nil {
    return err
}
defer permit.Release()
// Execute work, passing ctx to it separately.
```

This snippet belongs in an error-returning function. See the runnable
[tenant example](examples/tenants/example.go).

- Capacity must be positive. Queue limits must be nonnegative, and the per-tenant
  limit cannot exceed the global limit; otherwise `New` returns
  `ErrInvalidConfig`. Zero queue limits disable waiting. Limits are fixed.
- Pass zero or one tenant ID: `Acquire(ctx)` uses the default tenant, while
  `Acquire(ctx, "tenant-a")` selects an exact, nonempty string. A nil context
  panics; an explicit empty ID or multiple IDs returns `ErrInvalidTenant` before
  cancellation is checked. Use trusted, bounded IDs:
  Airlock does not authenticate callers or bound string lengths.
- If no immediate slot is available, full queues return `ErrQueueFull` or
  `ErrTenantQueueFull`. Global rejection takes precedence when both are full.
  An already-cancelled context returns its error before any capacity/queue check.
  Errors are compatible with `errors.Is`; failed acquisitions return no permit.
- Nonempty tenant queues take turns, with FIFO order inside each queue. New
  waiting tenants join the end of the rotation. An empty tenant queue is removed;
  when it becomes active again it rejoins at the end.
- Queue limits count pending requests only. One tenant may use all execution
  capacity when others are idle. Running work is never preempted. Fairness does
  not guarantee equal execution time or bounded latency, and cannot protect a
  new tenant from rejection when the global waiting queue is already full.
- Cancellation and granting are serialized. If cancellation wins, no token is
  retained. If granting wins, Acquire succeeds even if cancellation follows:
  release every successful permit. Cancellation does not stop admitted work.
- Use the returned `*Permit` without copying it. `Release` is concurrency-safe
  and idempotent; nil/zero permits do nothing. No automatic release occurs.
- `Stats()` returns one consistent `Stats` snapshot: `Capacity`, `InUse`,
  `Queued`, and `WaitingTenants`. It is not a completion barrier. Airlock must be
  constructed with `New` and must not be copied. It starts no goroutines.

The [design record](docs/proposals/001-tenant-fair-admission.md) describes the
contract. [Historical prototype measurements](docs/proposals/001-experiment-results.md)
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

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

## Migration

The sole admission type is `Airlock`, constructed with `New(Config)`. Every
`Acquire(ctx, tenantID)` acquires **one token**, returned by `Permit.Release()`.
Weighted requests, unlimited mode, and dynamic capacity are not implemented.

This is a breaking development change: `Pool`/`NewPool` and the earlier
`Gate`/`NewGate` names are removed. Existing callers must select explicit queue
limits, optionally supply a tenant ID, retain the returned permit, and release it.
`Stats().InUse` and `Stats().Capacity` replace `Num()` and `Size()` snapshots.
For a simple concurrency cap without tenant admission, use `x/sync/semaphore`.
No compatibility aliases are provided.

`Acquire` now has signature `Acquire(context.Context, ...string) (*Permit, error)`.
Existing direct calls with one tenant ID still work, but interfaces, mocks, and
method-value function types declaring the former fixed string parameter must
change to the variadic signature.

The original channel strategy is retained only as a test baseline. The initial
code migrated from `tr1v3r/pkg` commit
`b3e47bdfd27a149ecb4da01580b91be277b905f6` (`pools/`); Git history preserves it.
Use a caller-owned `sync.WaitGroup` to join tasks, including already-started tasks
when a later acquisition fails. Airlock does not execute jobs or track completion.

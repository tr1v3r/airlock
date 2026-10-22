# Airlock

[English](README.md) | [简体中文](README.zh-CN.md)

A standard-library-only, context-aware concurrency limiter for Go 1.24+.
The pool controls admission; callers own goroutines, results, errors, and completion.
It does not maintain workers, queue executable jobs, or start background goroutines.
Gate queues admission requests only.

## Migration status

This baseline was migrated from `tr1v3r/pkg` commit
`b3e47bdfd27a149ecb4da01580b91be277b905f6` (`pools/`).
It retains `NewPool`, `Acquire(ctx)`, `Release()`, `Num()` and `Size()`;
each acquisition holds one token. Weighted acquisition is planned, not implemented.
The `main` branch contains the baseline; ongoing development takes place on `dev`.
The breaking-change section below describes the history inherited from pools.

## Tenant-aware Gate

Use `Pool` for a simple concurrency cap. Use `Gate` when callers share capacity
but need FIFO waiting within each tenant, round-robin admission between waiting
tenants, and explicit waiting-queue limits. Both acquire **one token per call**;
weighted requests are not implemented.

```go
gate, err := airlock.NewGate(airlock.GateConfig{
    Capacity:           20,
    MaxQueued:          100,
    MaxQueuedPerTenant: 10,
})
if err != nil {
    return err
}
permit, err := gate.Acquire(ctx, tenantID)
if err != nil {
    return err
}
defer permit.Release()
// Execute work, passing ctx to it separately.
```

This snippet belongs in an error-returning function. See the runnable
[tenant example](examples/tenants/example.go).

- Capacity must be positive. Queue limits must be nonnegative, and the per-tenant
  limit cannot exceed the global limit; otherwise `NewGate` returns
  `ErrInvalidGateConfig`. Zero queue limits disable waiting. Limits are fixed.
- A tenant ID is an exact, nonempty string. A nil context panics; empty IDs return
  `ErrInvalidTenant` before cancellation is checked. Use trusted, bounded IDs:
  Gate does not authenticate callers or bound string lengths.
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
- `Stats()` returns one consistent `GateStats` snapshot: `Capacity`, `InUse`,
  `Queued`, and `WaitingTenants`. It is not a completion barrier. Gate must be
  constructed with `NewGate` and must not be copied. It starts no goroutines.

The [design record](docs/proposals/001-tenant-fair-admission.md) describes the
contract. [Historical prototype measurements](docs/proposals/001-experiment-results.md)
do not establish superiority over the strongest nested semaphore baseline or
production latency guarantees. The comparison workload now exercises the public
Gate API; `x/sync` remains a test-only dependency.

```sh
GOWORK=off AIRLOCK_LOAD=1 go test -v ./internal/comparison -run '^TestLoadComparison$' -count=1
GOWORK=off go test ./internal/comparison -run '^$' -bench BenchmarkAdmission -benchmem -count=3
```

## Installation

```sh
go get github.com/tr1v3r/airlock
```

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "sync"

    "github.com/tr1v3r/airlock"
)

func main() {
    p := airlock.NewPool(2)
    var wg sync.WaitGroup
    results := make([]int, 5)
    for i := range results {
        if err := p.Acquire(context.Background()); err != nil {
            log.Fatal(err)
        }
        wg.Add(1)
        go func() {
            defer wg.Done()
            defer p.Release()
            results[i] = (i + 1) * (i + 1)
        }()
    }
    wg.Wait()
    fmt.Println(results) // [1 4 9 16 25]
}
```

Acquire before spawning workers to bound their creation. Each worker writes to
its own result slot; read results after `wg.Wait`. Shared maps and appends need
separate synchronization. For request-scoped work, pass its context to `Acquire`.
If submission fails partway through, join already-started workers before returning;
the [batch example](examples/batch/example.go) demonstrates this with `defer wg.Wait()`.

## API

```go
type Pool interface {
    Acquire(ctx context.Context) error
    Release()
    Num() int
    Size() int
}
```

- `NewPool(size)`: positive capacity bounds concurrency; negative capacity disables
  the limit; zero panics. Use the constructor; the zero value is not a public API.
- `Acquire(ctx)`: blocks for capacity or returns a context error. Pass a non-nil
  context. `context.Background()` explicitly opts into waiting without a deadline.
  An already-cancelled context is rejected even when capacity is available or unlimited.
- Call `Release()` exactly once after each successful acquisition, usually with
  `defer`. On acquisition failure, do not release. Cancellation after success does
  not release capacity or stop the work; propagate the context to the work separately.
- If cancellation races with capacity becoming available, either outcome may win.
  The return value determines whether the caller owns a token.
- `Release()` is a no-op in unlimited mode. In bounded mode, it panics when no token
  is held. Tokens have no owner identity: releasing another worker's token cannot
  be detected, so correct pairing remains the caller's responsibility.
- `Num()` reports held bounded-pool tokens, excluding blocked requests. `Size()`
  reports the capacity. Both return zero in unlimited mode. Neither tracks completion.
- No `Close` is needed. Use `sync.WaitGroup` for joining work, and call `Add` before
  starting a worker and before beginning the completion wait for an idle group.
  The pool does not recover panics or aggregate errors.

## Cancellation

Inside an error-returning function with a pool `p` and parent context `ctx`:

```go
ctx, cancel := context.WithTimeout(ctx, time.Second)
defer cancel()
if err := p.Acquire(ctx); err != nil {
    return err // No token acquired; do not Release.
}
defer p.Release()
// Perform work here.
```

Import `context` and `time`. `Acquire` waits in the calling goroutine without
spawning an internal waiter. For asynchronous execution, launch and manage your
own goroutine; acquiring before launching usually provides better backpressure.

## Examples and validation

Run from `airlock/`:

| Example | Command |
| --- | --- |
| [Bounded batch and ordered results](examples/batch/example.go) | `go run ./examples/batch` |
| [Acquisition timeout](examples/cancellation/example.go) | `go run ./examples/cancellation` |
| [Caller-owned completion notification](examples/completion/example.go) | `go run ./examples/completion` |
| [Tenant-aware single-token admission](examples/tenants/example.go) | `go run ./examples/tenants` |

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

## Breaking migration

| Removed API | Replacement |
| --- | --- |
| `Wait()` | Handle the error from `Acquire(ctx)`; use `context.Background()` for unbounded waiting. |
| `WaitContext(ctx)` | `Acquire(ctx)` |
| `Done()` | `Release()` |
| `AsyncWait()` | Call `Acquire(ctx)` directly, or manage your own goroutine and its result. |
| `WaitAll()` | Caller-owned `sync.WaitGroup`. |
| `AsyncWaitAll()` | Caller-owned goroutine that waits on a `sync.WaitGroup` and closes a channel. |

Custom implementations and mocks must implement the new `Pool` interface.
Zero capacity now panics. Unlimited mode does not track completion, and neither
does bounded mode: move all batch completion tracking to the caller. Unmatched
bounded releases now panic instead of blocking. `Num` and `Size` remain available.

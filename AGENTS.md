# AGENTS.md

Airlock provides atomic weighted tenant admission through New(Config), Airlock,
Permit, and Stats. Acquire/AcquireFor remain single-token; AcquireN/AcquireNFor
request multiple tokens. It uses bounded waiting queues, tenant round-robin,
and protected head requests that cannot be bypassed while awaiting capacity.
TryAcquire/TryAcquireFor and TryAcquireN/TryAcquireNFor never queue or wait for
capacity; they return ErrWouldBlock when immediate acquisition would bypass a
waiter or exceed available capacity. They still acquire the internal mutex.
Optional Config.Priority adds static tenant-to-group mappings, weighted selection
turns, and reserved group waiting budgets. Group shares count request selections,
not tokens or execution time; a protected request blocks all groups. Pool and Gate
are no longer public APIs. Local permits have no lease. Package layout: the root
airlock package is the application API (New(Config)/Airlock/Permit/Stats plus
NewShared/Shared/SharedOptions over an injected protocol.Driver); protocol/ holds
the shared admission Driver contract, identities, validation, errors and the
optional reference state machine (ApplyCommand for structured state, ApplySnapshot
for JSON snapshots, ApplyOptimistic for the commit-window adapter); internal/scheduler
is the scheduling core shared by local admission and the reference state machine.
protocol never imports the root package; internal/scheduler imports neither; queue
and waiter internals are not public API. Memory is the reference
driver for tests and demos; ordinary local admission uses New(Config).
NewShared fills zero protocol settings with documented defaults, without changing
capacity or queue limits. Shared user and custom-driver guides are separate under
docs/guides; lifecycle supervision is application-owned example code.
PostgreSQL uses resource/request tables, protocol.ApplyCommand structured state and
delta writes;
Redis uses protocol.ApplyOptimistic guarded snapshot CAS
with backend time. Both are independent modules with explicit deployment limits;
drivers import github.com/tr1v3r/airlock/protocol, never the root package for the contract.

- Keep runtime dependencies limited to the standard library unless justified.
- Define atomic acquisition, cancellation, weight validation, release accounting,
  and fairness contracts before implementing the limiter.
- The limiter does not execute workers or track batch completion.
- Keep English and Chinese READMEs consistent and distinguish proposals from
  implemented behavior.
- Run gofmt, go test ./..., and go vet ./... for code changes; use race tests for
  concurrency implementations.
- Preserve unrelated changes. Keep development worktrees under .worktrees/.
- Keep the primary checkout on dev and clean; implement changes in a worktree.
- Use Conventional Commits. Do not create tags or releases without explicit
  user authorization.

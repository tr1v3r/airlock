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
are no longer public APIs. Distributed drivers remain proposals; implemented
permits are local and have no lease.

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

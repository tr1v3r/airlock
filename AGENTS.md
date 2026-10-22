# AGENTS.md

Airlock provides single-token tenant admission through New(Config), Airlock,
Permit, and Stats. It uses bounded waiting queues and round-robin scheduling.
Pool and Gate are no longer public APIs. Weighted acquisition is not implemented;
preserve single-token semantics until explicitly changed.

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

# AGENTS.md

Airlock is a standalone Go module for weighted concurrency admission.
The repository is currently a bootstrap; proposed APIs are not implemented.

- Keep runtime dependencies limited to the standard library unless justified.
- Define atomic acquisition, cancellation, weight validation, release accounting,
  and fairness contracts before implementing the limiter.
- The limiter does not execute workers or track batch completion.
- Keep English and Chinese READMEs consistent and distinguish proposals from
  implemented behavior.
- Run gofmt, go test ./..., and go vet ./... for code changes; use race tests for
  concurrency implementations.
- Preserve unrelated changes. Keep development worktrees under .worktrees/.
- Use Conventional Commits. Do not create tags or releases without explicit
  user authorization.

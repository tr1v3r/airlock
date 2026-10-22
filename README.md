# Airlock

Weighted concurrency control for Go.

[简体中文](README.zh-CN.md)

Airlock is a new, independent project exploring context-aware, weighted
concurrency admission. A task requests capacity according to its cost and
returns that capacity when it finishes.

## Status

Repository bootstrap only. No limiter implementation or stable public API is
available yet. Airlock evolves separately from `github.com/tr1v3r/pkg/pools`.

## Intended API (not implemented)

- `Acquire(ctx, n)`: acquire all requested capacity atomically, or return an error
  without retaining capacity.
- `Release(n)`: return previously acquired capacity.

Cancellation, invalid weights, queue fairness, and oversized requests will have
explicit contracts before implementation. Worker execution and completion
tracking remain the caller's responsibility.

## Development

Requires Go 1.24 or newer. Module: `github.com/tr1v3r/airlock`.

```sh
go test ./...
go vet ./...
```

## License

[MIT](LICENSE).

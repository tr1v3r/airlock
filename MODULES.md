# Module boundaries

| Directory | Module | Runtime dependencies |
| --- | --- | --- |
| `.` | `github.com/tr1v3r/airlock` | Standard library |
| `driver/memory` | `github.com/tr1v3r/airlock/driver/memory` | Core and standard library |

The root module contains local admission, the durable protocol, `Transition`, and
`NewDistributed`. It does not import drivers. Memory has no persistence and does
not share resources across driver instances. PostgreSQL and Redis are subsequent
independent modules; installing core must not pull in their client dependencies.

Each driver pins a reachable core commit containing its API. No placeholder
version or local-path replace is published. This PR bootstrap uses a pushed
commit's pseudo-version; it creates no tag/release. Formal releases require
separate authorization and must publish core before dependent drivers.

Run root checks and every nested module; root `go test ./...` excludes nested modules:

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
bash scripts/driver-check.sh
```

The driver check runs each module with `GOWORK=off`, verifies tidy, dependencies,
race tests, vet and per-file coverage. Shared backend contract tests live in
`internal/driverconformance`; this test helper is excluded from SDK statement
coverage, like comparison experiments. Actual implementations retain the 95%
per-file gate. Tests of a pinned driver also exercise that pinned core; root tests
exercise the working core. When core changes, advance pins before claiming that
independent drivers include those changes.

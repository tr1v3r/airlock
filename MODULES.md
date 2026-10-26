# Module boundaries

| Directory | Module | Runtime dependencies |
| --- | --- | --- |
| `.` | `github.com/tr1v3r/airlock` | Standard library |
| `driver/memory` | `github.com/tr1v3r/airlock/driver/memory` | Core and standard library |
| `driver/redis` | `github.com/tr1v3r/airlock/driver/redis` | Core and go-redis/v9 |
| `driver/postgres` | `github.com/tr1v3r/airlock/driver/postgres` | Core and database/sql; pgx/sqlmock are test-only |

The root module contains local admission, the durable protocol, `Transition`, and
`NewShared`. It does not import drivers. Memory is the Shared contract test/demo reference; use New(Config) for ordinary local admission. Memory has no persistence and does
not share resources across driver instances. PostgreSQL and Redis are implemented in independent modules; installing core must not pull in their client dependencies.

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

Import formatting is configured in the root `.golangci.yml`: standard library,
third-party dependencies, then `github.com/tr1v3r/airlock` packages. CI checks every
module. From the repository root, apply the same formatting locally with:

```sh
golangci-lint fmt
for manifest in driver/*/go.mod; do
  (cd "$(dirname "$manifest")" && golangci-lint fmt --config ../../.golangci.yml)
done
```

Use `fmt --diff` to check without rewriting files. CI pins golangci-lint v2.13.2.

The driver check runs each module with `GOWORK=off`, verifies tidy, dependencies,
race tests, vet and per-file coverage. Shared backend contract tests live in
`internal/driverconformance`; this test helper is excluded from SDK statement
coverage, like comparison experiments. Actual implementations retain the 95%
per-file gate. Tests of a pinned driver also exercise that pinned core; root tests
exercise the working core. When core changes, advance pins before claiming that
independent drivers include those changes.


PostgreSQL integration is opt-in locally and required on CI (16 and 18):

```sh
cd driver/postgres
AIRLOCK_POSTGRES_DSN='postgres://airlock:airlock@127.0.0.1:5432/airlock?sslmode=disable' GOWORK=off go test -race ./...
# From repository root, using isolated Docker containers:
bash scripts/postgres-failover.sh
```

The failover script creates its own network and containers, configures
`synchronous_commit=remote_apply`, fences the primary, promotes the standby and
verifies committed leases and a protected weighted queue survive. It cleans up
only its own fixtures. It does not prove safety of asynchronous replication,
unfenced promotion, storage rollback or arbitrary failover orchestration.


Redis integration is opt-in locally and required on CI (7 and 8):

```sh
cd driver/redis
AIRLOCK_REDIS_ADDR=127.0.0.1:6379 GOWORK=off go test -race ./...
# From repository root, using isolated Docker containers:
bash scripts/redis-cluster.sh
bash scripts/redis-failover.sh
```

Use an isolated test server: integration tests flush its script cache. The Cluster
script creates three primaries and maps Docker-internal addresses to published
loopback ports for macOS/Linux. The failover script deliberately demonstrates
acknowledged-grant loss with a stale asynchronous replica; passing means this
limitation was reproduced, not that failover is safe. See [ADR 007](docs/decisions/007-redis-driver.md).

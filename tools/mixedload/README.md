# Mixed Driver workload validation

This separate module contains an opt-in experiment harness; it changes no limiter
runtime. Run only against disposable services. Each trial uses a new Redis namespace
or PostgreSQL table pair and cleans up those owned resources. No shared tables are
truncated. Build with `GOWORK=off go build -o /tmp/airlock-mixedload .`.

`run.sh` requires REDIS_ADDR and POSTGRES_DSN, and runs six cases, three repetitions,
15 seconds per trial sequentially. Case/backend order varies between repetitions.
Reserve an otherwise quiet machine first. The runner rejects a nonempty output directory before starting any workload.
Set `OUTPUT_DIR` to a fresh raw directory; analyze its parent, for example:

```sh
result_root=$(mktemp -d /tmp/airlock-mixed-results.XXXXXX)
OUTPUT_DIR="$result_root/raw" ./run.sh
python3 analyze.py "$result_root"
```

The analyzer rejects duplicate trial identities and missing/inconsistent samples.
Run offline workflow regressions with `python3 test_workflow.py`.
Use `python3 analyze.py ../../docs/validation/mixed-capacity` only to regenerate
summaries from the already committed raw data.

The lifecycle is Driver.Submit → Driver.Observe while queued → Driver.Renew during
600ms of simulated holding → Driver.Release. All weights are 1. There is no real
business task, and this is not the Shared API's end-to-end latency. Each operation
has its own 500ms deadline; TTL is 2s, renewal spacing 200ms and polling spacing
50ms. Confirmed remaining duration is conservatively reduced by the complete
client round trip. A late renewal never extends a handle past its prior local
budget in this harness. Errors never replay the original mutation; Cancel on the
same request is a separately recorded cleanup operation.

Throughput counts business operations completing inside the fixed window; measured
latency uses that same completion cohort. Boundary operations and cleanup are
separate raw samples/counters. Retained sample storage and instrumentation are part
of the reported client heap/allocations. Redis bytes use MEMORY USAGE SAMPLES 0 on
the resource hash; PostgreSQL bytes include the trial's two relations and indexes,
with row and index bytes also reported separately. These are not comparable RSS.

Synthetic terminal history is seeded using backend time, validated via Transition,
and warmed through the ordinary driver mutation path before timing. Steady trials
use 60s retention and verify seed survival. expiry-gc trials seed 1,000 terminal
records and Capacity live grants plus Capacity queued requests; their retention,
lease and queue boundaries coincide at seed time + 2s. Setup reduces the offset
from measurement start; ExpiryBoundaryMS records that actual offset. Seeded grants
do not contribute to admitted lifecycle counts. End-state population checks are
not proof of continuous no-overadmission.

Quantiles sort samples and use zero-based floor(q*N), capped at N-1. p95 requires
20 samples, p99 requires 100; near those counts they are unstable near-max estimates.
Report trial ranges and N. expiry-windows.csv uses 500ms start-time cohorts around
the boundary to expose the concentrated maintenance cost rather than hiding it
inside the whole-trial percentile.

With 600ms holding, capacity/0.6s itself bounds steady lifecycle throughput before
backend cost. Closed-loop workers reduce offered load when the backend slows, so
this experiment explores service levels and pressure boundaries, not maximum
backend capacity, an open-loop overload threshold or a production SLO.

This is an independent Go module and is not automatically covered by the existing
root/driver CI jobs. Validate it manually with `GOWORK=off go test -race ./...`
and `GOWORK=off go vet ./...`; add an explicit tools check before maintaining it
as a long-lived CI target. Do not run capacity experiments in shared CI runners.

Recorded results are pinned to `819e030` and exercise Driver calls directly.
They do not cover Shared polling jitter introduced by #58 (`f739f5e`).
That change does not modify Driver, but future end-to-end Shared experiments
should include #58 and evaluate adaptive polling and failed cleanup together;
these results do not imply that the latest Shared implementation lacks jitter.

## Development layout fixtures

Current fixtures directly create the sole partitioned version-1 layout; warmup
validates storage but performs no migration. `-full-state-fixture` is only for
building/running the historical pre-partition baseline with this harness, not a
runtime compatibility mode. compare.py passes it only to the baseline binary.
The saved 72-trial terminal-history study is pinned to its original commits and
is not a performance measurement of later layout simplifications.

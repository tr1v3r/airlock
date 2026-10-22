#!/usr/bin/env bash
# Bounded functional stress/fuzz, not a throughput or latency benchmark.
set -euo pipefail
cd "$(dirname "$0")/.."
repo=$(pwd)
export GOWORK=off GOFLAGS=-mod=readonly
case "${1:-quick}" in
  quick)
    export AIRLOCK_STRESS=0
    count=1 cpus=1,4 fuzztime=5s
    concurrent_pattern='^Test(Local|SharedConcurrent|SharedClock)'
    ;;
  extended)
    export AIRLOCK_STRESS=1
    count=3 cpus=1,2,4 fuzztime=30s
    concurrent_pattern='^Test(Local|SharedClock)'
    ;;
  *) echo 'Usage: bash scripts/stress.sh {quick|extended}' >&2; exit 2 ;;
esac
# Fixed seeds and the Go shuffle seed are printed with verbose output for replay.
go test -race -v -shuffle=on -count="$count" -cpu="$cpus" -timeout=10m \
  -run "$concurrent_pattern" .
if [[ "${1:-quick}" == extended ]]; then
  # Shared already runs 128 clients across eight rounds. Do not multiply that
  # snapshot-heavy workload by the Local/clock CPU and repetition matrix.
  go test -race -v -shuffle=on -count=1 -cpu=1 -timeout=20m -run '^TestSharedConcurrent' .
fi
# The deterministic Transition model is serial. Repeating it for each CPU/count
# adds no interleaving coverage and multiplies snapshot work ninefold in extended.
go test -race -v -count=1 -timeout=3m -run '^TestTransitionModelTraces$' .
# Fuzzing has its own small worker budget; do not multiply it by the race CPU loop.
for target in FuzzLocalDynamicModel FuzzTransitionSnapshot FuzzTransitionCommandSequence FuzzOptimisticWindow; do
  go test -run '^$' -fuzz "^${target}$" -fuzztime="$fuzztime" -fuzzminimizetime=1s -parallel=2 -timeout=3m .
done
if [[ "${1:-quick}" == extended ]]; then
  # GOWORK=off intentionally tests each driver's published core pin, not local core.
  for manifest in "$repo"/driver/*/go.mod; do
    (cd "$(dirname "$manifest")" && go test -race -shuffle=on -count=3 -cpu=1,2,4 -timeout=5m ./...)
  done
fi

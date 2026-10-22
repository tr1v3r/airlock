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
    ;;
  extended)
    export AIRLOCK_STRESS=1
    count=3 cpus=1,2,4 fuzztime=30s
    ;;
  *) echo 'Usage: bash scripts/stress.sh {quick|extended}' >&2; exit 2 ;;
esac
# Fixed seeds and the Go shuffle seed are printed with verbose output for replay.
go test -race -v -shuffle=on -count="$count" -cpu="$cpus" -timeout=10m \
  -run '^Test(Local|SharedConcurrent|SharedClock|TransitionModel)' .
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

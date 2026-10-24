#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
repo=$(pwd)
export GOWORK=off GOFLAGS=-mod=readonly
for manifest in driver/*/go.mod; do
  [[ -f "$manifest" ]] || continue
  (
    cd "$(dirname "$manifest")"
    go mod tidy -diff
    go mod verify
    go vet ./...
    # Runnable examples are compiled, but aren't SDK runtime coverage targets.
    coverpkg=$(go list ./... | grep -v '/examples/' | paste -sd, -)
    go test -race -shuffle=on -count=1 -covermode=atomic -coverpkg="$coverpkg" -coverprofile=coverage.out ./...
    python3 "$repo/scripts/check_coverage.py" coverage.out
  )
done

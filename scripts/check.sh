#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export GOWORK=off
export GOFLAGS=-mod=readonly
case "${1:-}" in
  format)
    files=$(git ls-files '*.go' | while IFS= read -r file; do gofmt -l "$file"; done)
    if [[ -n "$files" ]]; then
      printf 'Run gofmt on:\n%s\n' "$files"
      exit 1
    fi
    ;;
  modules)
    go mod tidy
    git diff --exit-code -- go.mod go.sum
    go mod verify
    # The library and examples must retain their standard-library-only runtime.
    external=$(go list -deps -f '{{if and (not .Standard) (ne .Module.Path "github.com/tr1v3r/airlock")}}{{.ImportPath}}{{end}}' ./...)
    if [[ -n "$external" ]]; then
      printf 'Unexpected runtime dependencies:\n%s\n' "$external"
      exit 1
    fi
    ;;
  coverage)
    packages=$(go list ./... | awk '!/\/examples(\/|$)/ && !/\/internal\/comparison$/ && !/\/internal\/driverconformance$/')
    coverpkg=$(printf '%s\n' "$packages" | paste -sd, -)
    go test -race -shuffle=on -count=1 -timeout=5m -covermode=atomic -coverpkg="$coverpkg" -coverprofile=coverage.out ./...
    go tool cover -func=coverage.out
    python3 scripts/check_coverage.py coverage.out
    ;;
  examples)
    for dir in examples/*/; do go run "./$dir"; done
    ;;
  *) echo 'Usage: bash scripts/check.sh {format|modules|coverage|examples}' >&2; exit 2 ;;
esac

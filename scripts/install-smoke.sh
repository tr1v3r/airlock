#!/usr/bin/env bash
# External consumer smoke: the candidate must already be reachable via Go modules.
# Never uses a workspace or replace directive to disguise unpublished dependencies.
set -euo pipefail
if [[ $# != 1 || -z "$1" || "$1" == -* ]]; then
  echo 'Usage: bash scripts/install-smoke.sh <pushed-candidate-commit-or-version>' >&2
  exit 2
fi
candidate=$1
repo=$(cd "$(dirname "$0")/.." && pwd)
# Match literal documentation, never execute Markdown as a command.
for readme in "$repo/README.md" "$repo/README.zh-CN.md"; do
  if ! grep -Fxq 'go get github.com/tr1v3r/airlock@dev' "$readme" ||
     grep -Eq '^go get github\.com/tr1v3r/airlock(@latest)?$' "$readme"; then
    echo "Installation instructions must explicitly select @dev: $readme" >&2
    exit 1
  fi
done
export GOWORK=off
# This fresh consumer intentionally creates its own manifests, not the repository's.
export GOFLAGS=
fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/airlock-install-smoke.XXXXXX")
fixture_dir=$(cd "$fixture_dir" && pwd -P)
cleanup() {
  if [[ "$fixture_dir" = /*/airlock-install-smoke.* && -d "$fixture_dir" ]]; then
    rm -rf -- "${fixture_dir:?}"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for lane in documented candidate; do
  revision=dev
  [[ "$lane" != candidate ]] || revision=$candidate
  mkdir "$fixture_dir/$lane"
  (
    cd "$fixture_dir/$lane"
    go mod init example.com/airlock-install-smoke
    modules=(github.com/tr1v3r/airlock github.com/tr1v3r/airlock/driver/memory
             github.com/tr1v3r/airlock/driver/postgres github.com/tr1v3r/airlock/driver/redis)
    requests=()
    for module in "${modules[@]}"; do requests+=("$module@$revision"); done
    go get "${requests[@]}"
    # Verify MVS has not silently selected another core/driver version.
    for module in "${modules[@]}"; do
      expected=$(go list -m -f '{{.Version}}' "$module@$revision")
      selected=$(go list -m -f '{{.Version}}' "$module")
      if [[ "$expected" != "$selected" ]]; then
        printf 'Unexpected selected version for %s: wanted %s, got %s\n' "$module" "$expected" "$selected" >&2
        exit 1
      fi
      printf '%s: %s %s\n' "$lane" "$module" "$selected"
    done
    cat >main.go <<'GO'
package main

import (
    "context"
    "fmt"

    "github.com/tr1v3r/airlock"
    "github.com/tr1v3r/airlock/driver/memory"
    "github.com/tr1v3r/airlock/driver/postgres"
    redisdriver "github.com/tr1v3r/airlock/driver/redis"
)

func main() {
    local, err := airlock.New(airlock.Config{Capacity: 2})
    must(err)
    permit, err := local.TryAcquireNFor("online", 2)
    must(err)
    permit.Release()
    shared, err := airlock.NewShared(memory.New(), airlock.SharedOptions{
        Resource: "smoke", Config: airlock.ResourceConfig{Admission: airlock.Config{Capacity: 2}},
    })
    must(err)
    lease, err := shared.AcquireNFor(context.Background(), "online", 2)
    must(err)
    must(lease.Release(context.Background()))
    // Compile remote adapters without contacting a database or Redis server.
    _ = postgres.New
    _ = redisdriver.New
    fmt.Println("external consumer: local weighted and Shared defaults OK")
}
func must(err error) { if err != nil { panic(err) } }
GO
    if [[ "$lane" == candidate ]]; then
      # This field belongs to the candidate and need not exist on dev before merge.
      cat >candidate.go <<'GO'
package main
import "github.com/tr1v3r/airlock"
var _ = airlock.SharedOptions{ClockSkewAllowance: 0}
GO
    fi
    go run .
    go mod verify
  )
done

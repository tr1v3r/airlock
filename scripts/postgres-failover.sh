#!/usr/bin/env bash
# Isolated, destructive only to containers/network created by this invocation.
set -euo pipefail
cd "$(dirname "$0")/.."
repo=$(pwd)
image=${AIRLOCK_PG_IMAGE:-postgres:16}
prefix="airlock-pg-failover-$$"
primary_name="$prefix-primary"
standby_name="$prefix-standby"
primary='' standby='' network=''
fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/airlock-pg-failover.XXXXXX")
fixture_dir=$(cd "$fixture_dir" && pwd -P)
cleanup() {
  # Names are candidates, not proof of ownership. Record IDs only after success.
  [[ -z "$standby" ]] || docker rm -f "$standby" >/dev/null 2>&1 || true
  [[ -z "$primary" ]] || docker rm -f "$primary" >/dev/null 2>&1 || true
  [[ -z "$network" ]] || docker network rm "$network" >/dev/null 2>&1 || true
  if [[ "$fixture_dir" = /*/airlock-pg-failover.* && -d "$fixture_dir" ]]; then
    rm -rf -- "${fixture_dir:?}"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
wait_ready() {
  for ((i=0;i<120;i++)); do
    if docker exec "$1" pg_isready -h 127.0.0.1 -U airlock -d airlock >/dev/null 2>&1; then return; fi
    sleep 0.5
  done
  docker logs "$1"
  return 1
}
created=$(docker network create "$prefix")
network=$created
created=$(docker run --rm -d --name "$primary_name" --network "$network" \
  -p 127.0.0.1::5432 -e POSTGRES_USER=airlock -e POSTGRES_PASSWORD=airlock \
  -e POSTGRES_DB=airlock "$image")
primary=$created
wait_ready "$primary"
docker exec "$primary" sh -c 'printf "host replication all 0.0.0.0/0 scram-sha-256\n" >> "$PGDATA/pg_hba.conf"'
docker exec "$primary" psql -U airlock -d airlock -v ON_ERROR_STOP=1 -c 'SELECT pg_reload_conf()' >/dev/null
created=$(docker run --rm -d --name "$standby_name" --network "$network" --user postgres \
  -p 127.0.0.1::5432 -e PGDATA=/tmp/airlock-standby -e PGPASSWORD=airlock \
  --entrypoint bash "$image" -c \
  'pg_basebackup -D "$PGDATA" -d "host='"$primary_name"' user=airlock application_name=airlock_standby" -R -X stream && exec postgres -D "$PGDATA"')
standby=$created
wait_ready "$standby"
docker exec "$primary" psql -U airlock -d airlock -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (airlock_standby)'" >/dev/null
docker exec "$primary" psql -U airlock -d airlock -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET synchronous_commit = 'remote_apply'" >/dev/null
docker exec "$primary" psql -U airlock -d airlock -v ON_ERROR_STOP=1 -c 'SELECT pg_reload_conf()' >/dev/null
for ((i=0;i<100;i++)); do
  sync_state=$(docker exec "$primary" psql -U airlock -d airlock -Atc "SELECT sync_state FROM pg_stat_replication WHERE application_name='airlock_standby'")
  [[ "$sync_state" == sync ]] && break
  sleep 0.1
done
[[ "$sync_state" == sync ]]
primary_port=$(docker port "$primary" 5432/tcp | sed 's/.*://')
standby_port=$(docker port "$standby" 5432/tcp | sed 's/.*://')
export AIRLOCK_PG_FAILOVER_FIXTURE="$fixture_dir/lease.json" GOWORK=off
cd "$repo/driver/postgres"
AIRLOCK_POSTGRES_DSN="postgres://airlock:airlock@127.0.0.1:$primary_port/airlock?sslmode=disable" \
AIRLOCK_PG_FAILOVER_MODE=seed go test -race -count=1 -run '^TestSynchronousFailover$' -v .
# Fence the old primary before promotion. It is never restarted in this test.
docker stop --time 5 "$primary" >/dev/null
docker exec "$standby" pg_ctl promote -D /tmp/airlock-standby -w >/dev/null
AIRLOCK_POSTGRES_DSN="postgres://airlock:airlock@127.0.0.1:$standby_port/airlock?sslmode=disable" \
AIRLOCK_PG_FAILOVER_MODE=verify go test -race -count=1 -run '^TestSynchronousFailover$' -v .
printf 'Verified committed weighted leases and protected queue survive fenced synchronous failover (%s).\n' "$image"

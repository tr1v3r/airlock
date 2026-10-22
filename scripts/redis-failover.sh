#!/usr/bin/env bash
# Demonstrates acknowledged-state loss with asynchronous Redis replication.
# This is an expected limitation test, not a claim of failover safety.
set -euo pipefail
cd "$(dirname "$0")/.."
image=${AIRLOCK_REDIS_IMAGE:-redis:7-alpine}
prefix="airlock-redis-failover-$$"
primary="$prefix-primary"
replica="$prefix-replica"
fixture_dir=$(mktemp -d)
cleanup() {
  docker unpause "$replica" >/dev/null 2>&1 || true
  docker rm -f "$primary" "$replica" >/dev/null 2>&1 || true
  docker network rm "$prefix" >/dev/null 2>&1 || true
  rm -rf "$fixture_dir"
}
trap cleanup EXIT
ready() {
  for ((i=0;i<120;i++)); do
    if docker exec "$1" redis-cli ping 2>/dev/null | grep -q PONG; then return; fi
    sleep 0.5
  done
  docker logs "$1"
  return 1
}
docker network create "$prefix" >/dev/null
docker run --rm -d --name "$primary" --network "$prefix" -p 127.0.0.1::6379 \
  "$image" redis-server --save '' --appendonly no --maxmemory-policy noeviction >/dev/null
ready "$primary"
docker run --rm -d --name "$replica" --network "$prefix" -p 127.0.0.1::6379 \
  "$image" redis-server --save '' --appendonly no --replicaof "$primary" 6379 >/dev/null
ready "$replica"
synced=false
for ((i=0;i<120;i++)); do
  if docker exec "$replica" redis-cli info replication | grep -q 'master_link_status:up'; then synced=true; break; fi
  sleep 0.5
done
"$synced"
primary_port=$(docker port "$primary" 6379/tcp | sed 's/.*://')
replica_port=$(docker port "$replica" 6379/tcp | sed 's/.*://')
# Precompile before pausing, keeping the fault window short and reproducible.
(cd driver/redis && GOWORK=off go test -race -c -o "$fixture_dir/redis.test" .)
docker pause "$replica" >/dev/null
# Disconnect its replication socket before the write, so no unread grant can be
# applied on resume. Pausing alone would leave socket-buffered data ambiguous.
docker exec "$primary" redis-cli client kill type replica >/dev/null
docker exec "$primary" redis-cli info replication | grep -q 'connected_slaves:0'
export AIRLOCK_REDIS_FAILOVER_FIXTURE="$fixture_dir/lease.json"
AIRLOCK_REDIS_ADDR="127.0.0.1:$primary_port" AIRLOCK_REDIS_FAILOVER_MODE=seed \
  "$fixture_dir/redis.test" -test.run '^TestAsynchronousFailoverLosesAcknowledgedGrant$' -test.v
# Fence the old primary while the replica remains paused; its unread socket is lost.
docker stop --time 1 "$primary" >/dev/null
docker unpause "$replica" >/dev/null
docker exec "$replica" redis-cli replicaof no one
AIRLOCK_REDIS_ADDR="127.0.0.1:$replica_port" AIRLOCK_REDIS_FAILOVER_MODE=verify \
  "$fixture_dir/redis.test" -test.run '^TestAsynchronousFailoverLosesAcknowledgedGrant$' -test.v

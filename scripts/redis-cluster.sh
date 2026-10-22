#!/usr/bin/env bash
# Creates and removes only this invocation's isolated three-primary Cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
image=${AIRLOCK_REDIS_IMAGE:-redis:7-alpine}
prefix="airlock-redis-cluster-$$"
nodes=()
network=
cleanup() {
  # Empty-array expansion must also work with macOS's Bash 3 and nounset.
  for node in "${nodes[@]:-}"; do
    [[ -z "$node" ]] || docker rm -f "$node" >/dev/null 2>&1 || true
  done
  [[ -z "$network" ]] || docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
created=$(docker network create "$prefix")
network=$created
addresses=()
seeds=()
mapping=()
for i in 1 2 3; do
  node=$(docker run --rm -d --name "$prefix-$i" --network "$network" -p 127.0.0.1::6379 \
    "$image" redis-server --cluster-enabled yes --cluster-node-timeout 5000 \
    --save '' --appendonly no --maxmemory-policy noeviction)
  # Append only after docker run succeeded and returned this container's ID.
  nodes+=("$node")
  ready=false
  for ((j=0;j<120;j++)); do
    if docker exec "$node" redis-cli ping 2>/dev/null | grep -q PONG; then ready=true; break; fi
    sleep 0.5
  done
  "$ready"
  ip=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$node")
  port=$(docker port "$node" 6379/tcp | sed 's/.*://')
  addresses+=("$ip:6379")
  seeds+=("127.0.0.1:$port")
  mapping+=("$ip:6379=127.0.0.1:$port")
done
docker exec "${nodes[0]}" redis-cli --cluster create "${addresses[@]}" --cluster-replicas 0 --cluster-yes
for node in "${nodes[@]}"; do
  ready=false
  for ((j=0;j<120;j++)); do
    if docker exec "$node" redis-cli cluster info | grep -q 'cluster_state:ok'; then ready=true; break; fi
    sleep 0.5
  done
  "$ready"
done
export AIRLOCK_REDIS_CLUSTER_ADDRS AIRLOCK_REDIS_CLUSTER_MAPPING GOWORK=off
AIRLOCK_REDIS_CLUSTER_ADDRS=$(IFS=,; echo "${seeds[*]}")
AIRLOCK_REDIS_CLUSTER_MAPPING=$(IFS=,; echo "${mapping[*]}")
cd driver/redis
go test -race -count=1 -timeout=5m -run '^TestIntegrationCluster$' -v .

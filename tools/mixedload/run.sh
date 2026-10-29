#!/bin/sh
# Run sequential trials only after reserving an otherwise quiet measurement window.
set -eu
: "${REDIS_ADDR:?disposable Redis required}"
: "${POSTGRES_DSN:?disposable PostgreSQL required}"
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
out=${OUTPUT_DIR:-"$root/docs/validation/mixed-capacity/raw"}
mkdir -p "$out"
binary=${MIXEDLOAD_BINARY:-/tmp/airlock-mixedload}
duration=${TRIAL_DURATION:-15s}
repeats=${TRIAL_REPEATS:-3}
i=1
while [ "$i" -le "$repeats" ]; do
 case "$i" in 1) order='0 1 2 3 4 5';; 2) order='5 4 3 2 1 0';; *) order='2 3 4 5 0 1';; esac
 for n in $order; do
  scenario=steady
  case "$n" in
   0) history=0; capacity=8; workers=8;;
   1) history=0; capacity=8; workers=32;;
   2) history=1000; capacity=8; workers=32;;
   3) history=10000; capacity=8; workers=32;;
   4) history=1000; capacity=32; workers=64;;
   5) history=1000; capacity=32; workers=32; scenario=expiry-gc;;
  esac
  if [ "$i" = 2 ]; then backends='postgres redis'; else backends='redis postgres'; fi
  for backend in $backends; do
   if [ "$backend" = redis ]; then address=$REDIS_ADDR; else address=$POSTGRES_DSN; fi
   name="$backend-case$n-run$i"
   "$binary" -backend "$backend" -address "$address" -scenario "$scenario" -history "$history" -capacity "$capacity" -workers "$workers" -repeat "$i" -duration "$duration" -output "$out/$name.jsonl" > "$out/$name.summary.json"
   gzip -n "$out/$name.jsonl"
   python3 -c 'import json,sys; x=json.load(open(sys.argv[1])); print(sys.argv[1].split("/")[-1], "ops/s=%.1f admit/s=%.2f errors=%s unusable=%d invariants=%s"%(x["OperationsPerSecond"],x["AdmissionsPerSecond"],x["Errors"],x["UnusableGrants"],x["InvariantErrors"]),flush=True)' "$out/$name.summary.json"
  done
 done
 i=$((i+1))
done

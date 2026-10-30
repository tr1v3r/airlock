#!/usr/bin/env python3
"""Interleave fixed-workload baseline/candidate trials on disposable backends."""
import argparse
import gzip
import json
import os
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("output", type=Path)
parser.add_argument("--baseline", required=True, type=Path)
parser.add_argument("--candidate", required=True, type=Path)
parser.add_argument("--duration", default="15s")
parser.add_argument("--repeats", type=int, default=3)
args = parser.parse_args()
if args.repeats < 1:
    parser.error("repeats must be positive")
if args.output.exists() and any(args.output.iterdir()):
    parser.error("output must be new or empty")
addresses = {"redis": os.environ["REDIS_ADDR"], "postgres": os.environ["POSTGRES_DSN"]}
cases = [(0, 8, 8), (0, 8, 32), (1000, 8, 32), (10000, 8, 32),
         (1000, 32, 64), (1000, 32, 32)]
args.output.mkdir(parents=True, exist_ok=True)
for variant in ("baseline", "candidate"):
    (args.output / variant / "raw").mkdir(parents=True, exist_ok=True)
for repeat in range(1, args.repeats + 1):
    order = list(range(6)) if repeat == 1 else list(reversed(range(6))) if repeat == 2 else [2, 3, 4, 5, 0, 1]
    for case in order:
        history, capacity, workers = cases[case]
        for backend in (("postgres", "redis") if repeat == 2 else ("redis", "postgres")):
            for variant in (("candidate", "baseline") if repeat % 2 == 0 else ("baseline", "candidate")):
                name = f"{backend}-case{case}-run{repeat}"
                root = args.output / variant / "raw"
                raw = root / f"{name}.jsonl"
                cmd = [str(getattr(args, variant).resolve()), "-backend", backend,
                       "-address", addresses[backend], "-scenario", "expiry-gc" if case == 5 else "steady",
                       "-history", str(history), "-capacity", str(capacity), "-workers", str(workers),
                       "-repeat", str(repeat), "-duration", args.duration, "-output", str(raw)]
                if variant == "baseline":
                    cmd.append("-full-state-fixture")
                begin = time.time()
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
                (root / f"{name}.stderr.txt").write_text(result.stderr)
                (root / f"{name}.summary.json").write_text(result.stdout)
                if result.returncode:
                    raise SystemExit(f"{variant}/{name} failed: {result.returncode}; samples retained")
                summary = json.loads(result.stdout)
                with raw.open("rb") as source, (root / f"{name}.jsonl.gz").open("wb") as target:
                    with gzip.GzipFile(fileobj=target, mode="wb", mtime=0) as compressed:
                        compressed.write(source.read())
                raw.unlink()
                print(json.dumps({"variant": variant, "trial": name, "elapsed": time.time()-begin,
                                  "ops_per_second": summary["OperationsPerSecond"],
                                  "admissions_per_second": summary["AdmissionsPerSecond"],
                                  "errors": summary["Errors"], "invariants": summary["InvariantErrors"]}), flush=True)

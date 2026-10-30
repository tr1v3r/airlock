#!/usr/bin/env python3
"""Validate matched comparison trials and export a compact per-trial CSV."""
import argparse
import csv
import gzip
import json
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("runs", type=Path)
parser.add_argument("--repeats", type=int, default=3)
args = parser.parse_args()
if args.repeats < 1:
    parser.error("repeats must be positive")
cases = [(0, 8, 8), (0, 8, 32), (1000, 8, 32), (10000, 8, 32), (1000, 32, 64), (1000, 32, 32)]
reports = {}
latencies = {}
for variant in ("baseline", "candidate"):
    root = args.runs / variant
    expected = {f"{backend}-case{case}-run{repeat}" for backend in ("redis", "postgres")
                for case in range(6) for repeat in range(1, args.repeats + 1)}
    paths = list((root / "raw").glob("*.summary.json"))
    actual = {p.name.removesuffix(".summary.json") for p in paths}
    if actual != expected:
        raise ValueError(f"{variant}: missing or unexpected trial files: {expected ^ actual}")
    raw_paths = list((root / "raw").glob("*.jsonl*"))
    raw_by_name = {p.name.removesuffix(".gz").removesuffix(".jsonl"): p for p in raw_paths}
    if set(raw_by_name) != expected or len(raw_paths) != len(expected):
        raise ValueError(f"{variant}: missing, duplicate or unexpected raw trials")
    for sidecar in paths:
        raw_path = raw_by_name[sidecar.name.removesuffix(".summary.json")]
        opener = gzip.open if raw_path.suffix == ".gz" else open
        with opener(raw_path, "rt") as f:
            last = None
            for line in f:
                last = json.loads(line)
        if last != json.loads(sidecar.read_text()):
            raise ValueError(f"{variant}: raw/sidecar summary mismatch: {sidecar.name}")
    subprocess.run([sys.executable, str(Path(__file__).with_name("analyze.py")), str(root)],
                   check=True, stdout=subprocess.DEVNULL)
    reports[variant] = {p.name.removesuffix(".summary.json"): json.loads(p.read_text()) for p in paths}
    with (root / "latency.csv").open() as f:
        latencies[variant] = {r["trial"].removesuffix(".jsonl.gz"): r
                              for r in csv.DictReader(f) if r["operation/result"] == "renew/all"}
rows = []
for name in sorted(reports["baseline"]):
    before, after = (reports[v][name] for v in ("baseline", "candidate"))
    for field in ("Backend", "Scenario", "History", "Capacity", "Workers", "Repeat",
                  "DurationSeconds", "Settings", "GoVersion", "GOMAXPROCS"):
        if before[field] != after[field]:
            raise ValueError(f"{name}: mismatched workload field {field}")
    case = int(name.split("-case")[1].split("-")[0])
    if tuple(before[k] for k in ("History", "Capacity", "Workers")) != cases[case]:
        raise ValueError(f"{name}: unexpected case parameters")
    for field in ("Records", "SeedHistory", "SeedActive", "SeedQueued", "SeedExpired"):
        if before["Before"][field] != after["Before"][field]:
            raise ValueError(f"{name}: unmatched initial population {field}")
    for variant in ("baseline", "candidate"):
        r = reports[variant][name]
        errors = sum(r["Errors"].values())
        if errors + r["Successes"] != r["Operations"] or r["InvariantErrors"] or r["UnusableGrants"]:
            raise ValueError(f"{variant}/{name}: accounting or observed grant invariant failed")
        latency = latencies[variant].get(name, {})
        rows.append([name, variant, r["Backend"], case, r["Repeat"], r["Operations"], errors,
                     100 * errors / r["Operations"] if r["Operations"] else 0,
                     r["AdmissionsPerSecond"], r["OperationsPerSecond"], latency.get("n", 0),
                     latency.get("p95_ms", ""), latency.get("p99_ms", ""),
                     r["Allocated"] / 1e9, r["PeakSampledHeap"] / 2**20,
                     r["Before"]["Bytes"], r["After"]["Bytes"], r["CleanupErrors"]])
with (args.runs / "comparison.csv").open("w", newline="") as f:
    writer = csv.writer(f, lineterminator="\n")
    writer.writerow(["trial", "variant", "backend", "case", "repeat", "operations", "errors",
                     "error_percent", "admissions_per_second", "operations_per_second", "renew_n",
                     "renew_p95_ms", "renew_p99_ms", "allocated_gb", "peak_heap_mib",
                     "before_backend_bytes", "after_backend_bytes", "cleanup_errors"])
    writer.writerows(rows)
print(f"Validated {len(rows)} matched trial rows; wrote {args.runs / 'comparison.csv'}")

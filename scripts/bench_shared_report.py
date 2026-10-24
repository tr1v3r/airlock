#!/usr/bin/env python3
"""bench_shared_report.py — 从 bench-results/raw/ 解析基准输出,生成
shared-bench.csv 与 summary.md 骨架(观察结论由分析员复核补充)。

输入:scripts/bench-shared.sh 产出的 raw 文件(头部含 '# benchtime: Nx')、
plan.tsv(family W H count benchtime outcome,同 key 取最后一行)、
run-env.txt(环境块)。

规则:
  * 每 (family,W,H,count) 优先取非 retry 文件;两者皆有结果行时以非 retry
    为准(retry 是 ErrCorruptState 单次重试的存证,同时进「环境事件」)。
  * 结果行解析:首 token 为 Benchmark 名(剥掉 -<GOMAXPROCS> 尾缀),其后
    成对 (value unit);iterations 单独入 CSV 供 b.N 披露。
  * 固定 Nx 下 b.N==N;结果行 iterations 与计划 N 不一致 → 环境事件。
  * contended 深 cell(W>capacity=8)计划 N<4W → 自动披露「测得部分爬坡段」。
  * 模型校验:uncontended ns/op ≈ 2×t_trans(steady_records),t_trans ≈
    70µs + 3.5µs/record(audit §1.8 校准);偏离 >2× 或 <0.5× 标可疑。
"""

from __future__ import annotations

import argparse
import re
import statistics
import sys
from pathlib import Path

FAMILY_LABEL = {
    "shared-uncontended": "root benchDriver · uncontended",
    "shared-contended": "root benchDriver · contended (capacity=8)",
    "memory-uncontended": "memory.Driver · uncontended",
    "memory-contended": "memory.Driver · contended (capacity=8)",
}
FAMILY_ORDER = list(FAMILY_LABEL)

CAPACITY = 8  # contended family 的固定容量

FILE_RE = re.compile(
    r"^(?P<fam>[a-z][a-z-]*[a-z])_W(?P<w>\d+)_H(?P<h>\d+)"
    r"_c(?P<c>\d+)(?P<retry>\.retry)?\.txt$"
)
RESULT_RE = re.compile(r"^(?P<name>Benchmark\S+)\s+(?P<iters>\d+)\s+(?P<rest>.+)$")
BT_RE = re.compile(r"^# benchtime:\s*(\d+)x")

EVENT_PATTERNS = (
    "ErrCorruptState",
    "corrupt-state dump",
    "--- SKIP",
    "SKIP:",
    "panic:",
    "no tests to run",
    "FAIL",
)


def parse_result_line(line: str):
    m = RESULT_RE.match(line)
    if not m:
        return None
    token, iters, rest = m.group("name"), m.group("iters"), m.group("rest")
    if "-" in token:  # 剥 -<GOMAXPROCS>(名字本身不含 '-')
        token = token.rsplit("-", 1)[0]
    wm = re.search(r"/W=(\d+)", token)
    hm = re.search(r"/H=(\d+)", token)
    if not wm:
        return None
    metrics: dict[str, float] = {"iterations": float(iters)}
    fields = rest.split()
    for i in range(0, len(fields) - 1, 2):
        try:
            value = float(fields[i])
        except ValueError:
            continue
        metrics[fields[i + 1]] = value
    return {
        "benchmark": token,
        "W": int(wm.group(1)),
        "H": int(hm.group(1)) if hm else 0,
        "metrics": metrics,
    }


def load_raw_file(path: Path):
    """返回 (benchtime_or_None, [result dicts], event_lines)。"""
    benchtime = None
    results, events = [], []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        bm = BT_RE.match(line)
        if bm:
            benchtime = int(bm.group(1))
        if line.startswith("Benchmark"):
            parsed = parse_result_line(line)
            if parsed:
                results.append(parsed)
        if any(p in line for p in EVENT_PATTERNS):
            events.append(line.strip())
    return benchtime, results, events


def load_plan(path: Path):
    plan: dict[tuple, dict] = {}
    if not path.exists():
        return plan
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.split("\t")
        if len(parts) != 6:
            continue
        fam, w, h, c, bt, outcome = parts
        key = (fam, int(w), int(h), int(c))
        m = re.match(r"(\d+)x", bt)
        plan[key] = {
            "N": int(m.group(1)) if m else None,
            "outcome": outcome,
            "benchtime": bt,
        }
    return plan


def fmt_count(v: float) -> str:
    return f"{v:,.1f}"


def fmt_ns(v: float) -> str:
    if v >= 1e6:
        return f"{v / 1e6:,.2f}ms"
    if v >= 1e3:
        return f"{v / 1e3:,.1f}µs"
    return f"{v:,.0f}ns"


def median_or(d, key):
    vals = [m["metrics"][key] for m in d if key in m["metrics"]]
    return statistics.median(vals) if vals else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", required=True, type=Path)
    ap.add_argument("--plan", required=True, type=Path)
    ap.add_argument("--env-file", required=True, type=Path)
    ap.add_argument("--out-csv", required=True, type=Path)
    ap.add_argument("--out-md", required=True, type=Path)
    args = ap.parse_args()

    plan = load_plan(args.plan)

    # cells: (fam,W,H,c) -> {"benchtime":N,"results":[...],"from_retry":bool}
    cells: dict[tuple, dict] = {}
    all_events: list[tuple[str, str]] = []
    retry_evidence: list[str] = []
    for f in sorted(args.raw_dir.glob("*.txt")):
        fm = FILE_RE.match(f.name)
        if not fm:
            bt, _res, evts = load_raw_file(f)
            all_events.extend((f.name, e) for e in evts)
            continue
        fam = fm.group("fam")
        if fam not in FAMILY_LABEL:
            continue
        key = (fam, int(fm.group("w")), int(fm.group("h")), int(fm.group("c")))
        bt, results, evts = load_raw_file(f)
        all_events.extend((f.name, e) for e in evts)
        if fm.group("retry"):
            retry_evidence.append(f.name)
        # 防御:结果行的 W/H 必须与文件名一致,否则说明正则污染(如 'W=1'
        # 子串命中 'W=16'),弃用该文件并记事件,续跑会重跑该 cell。
        if any(r["W"] != key[1] or r["H"] != key[2] for r in results):
            bad = next(r for r in results if r["W"] != key[1] or r["H"] != key[2])
            all_events.append(
                (f.name, f"REGEX-CONTAMINATION: 文件名 W={key[1]}/H={key[2]} "
                 f"但含 {bad['benchmark']} 结果行,弃用"))
            continue
        if not results:
            continue
        cur = cells.get(key)
        is_retry = bool(fm.group("retry"))
        if cur is None or (is_retry is False and cur["from_retry"]):
            # 非 retry 文件优先:retry 只是 ErrCorruptState 重试的存证。
            cells[key] = {"benchtime": bt, "results": results, "from_retry": is_retry}

    # ---- CSV ----
    rows = []
    for (fam, w, h, c), cell in sorted(cells.items()):
        parsed = cell["results"][0]
        for metric, value in parsed["metrics"].items():
            rows.append(
                (parsed["benchmark"], fam, w, h, c, metric, value)
            )
    with args.out_csv.open("w", encoding="utf-8") as fh:
        fh.write("benchmark,family,W,H,count,metric,value\n")
        for benchmark, fam, w, h, c, metric, value in rows:
            fh.write(
                f"{benchmark},{fam},{w},{h},{c},{metric},{value!r}\n"
            )

    # ---- 聚合(多 count 取中位)----
    # agg[fam][(W,H)][metric] = median
    agg: dict[str, dict[tuple, dict[str, float]]] = {f: {} for f in FAMILY_ORDER}
    counts_done: dict[tuple, int] = {}
    for (fam, w, h, c), cell in sorted(cells.items()):
        point = agg[fam].setdefault((w, h), {})
        for m in cell["results"]:
            for k, v in m["metrics"].items():
                point.setdefault(k, []).append(v)
        counts_done[(fam, w, h)] = counts_done.get((fam, w, h), 0) + 1
    for fam in agg:
        for point in agg[fam].values():
            for k in list(point):
                point[k] = statistics.median(point[k])

    def table(fam: str, metric: str, fmt=fmt_count) -> str:
        pts = [k for k in agg[fam] if metric in agg[fam][k]]
        if not pts:
            return "_无数据_\n"
        ws = sorted({k[0] for k in pts})
        hs = sorted({k[1] for k in pts})
        out = ["| W\\H | " + " | ".join(str(h) for h in hs) + " |",
               "|---|" + "---|" * len(hs)]
        for w in ws:
            row = []
            for h in hs:
                v = agg[fam].get((w, h), {}).get(metric)
                row.append(fmt(v) if v is not None else "—")
            out.append(f"| {w} | " + " | ".join(row) + " |")
        return "\n".join(out) + "\n"

    md: list[str] = []
    md.append("# Shared 基准矩阵报告\n")
    md.append("> 由 `scripts/bench-shared.sh` + `scripts/bench_shared_report.py` 生成;"
              "「观察结论」由分析员复核后定稿。\n")

    md.append("## 1. 环境\n")
    if args.env_file.exists():
        md.append("```")
        md.append(args.env_file.read_text(encoding="utf-8").rstrip())
        md.append("```\n")

    # ---- 参数与取舍 ----
    md.append("## 2. 参数与取舍\n")
    md.append("- benchtime 全部固定 `-benchtime=Nx`(不用时间型,cell 间 b.N 可比)。"
              "uncontended 与 memory:2000x;contended 小 H(W≤64 或 H≤128):2000x;"
              "contended 深队列(W=256,H≥1000):1200x(≥4×256,满足深 cell 稳态规则 N≥4W)。\n")
    count1 = sorted({(f, w, h) for (f, w, h, c) in cells if counts_done.get((f, w, h), 0) == 1
                     and f in ("shared-uncontended", "shared-contended")
                     and cells.get((f, w, h, 1), {}).get("benchtime") not in (100, None)})
    if count1:
        lst = ", ".join(f"{f.split('-')[1]} W={w}/H={h}" for f, w, h in count1)
        md.append(f"- count=1 的 cell(时长取舍,砍 count 保 N):{lst}。"
                  "预算参考:W=256/H=1000@1200x≈3min、H=5000≈12min、H=10000-clamped≈18min;"
                  "其余 cell 默认 count=3(多 count 取中位)。\n")
    md.append("- 显式 -timeout:contended 族 80m(≥2× 最坏派生 MaxWait ≈39min,"
              "W=256×H=10000-clamped;go test 默认 10m 会在 H≥5000 深 cell 中途 panic);"
              "uncontended/memory 20m。\n")
    md.append("- 每 cell 独立 `go test -run '^$' -bench '<斜杠锚定正则>$' -count=1`,"
              "输出(含命令行+时间戳头部)tee 到 `bench-results/raw/`;ErrCorruptState 失败"
              "单 cell 重试一次(已知墙钟后跳 fail-closed 偶发,dump 行保留在 raw)。\n")

    # ---- 透视表 ----
    md.append("## 3. 透视表(多 count 取中位)\n")
    md.append("### 3.1 吞吐 cycles/s\n")
    for fam in FAMILY_ORDER:
        if not agg[fam]:
            continue
        md.append(f"#### {FAMILY_LABEL[fam]}\n")
        md.append(table(fam, "cycles_per_s"))
    md.append("### 3.2 Acquire 延迟(p50 / p99 / p999 / max)\n")
    for fam in ("shared-uncontended", "shared-contended"):
        if not agg[fam]:
            continue
        md.append(f"#### {FAMILY_LABEL[fam]}\n")
        for metric, label in (("latency_p50_ns", "p50"), ("latency_p99_ns", "p99"),
                              ("latency_p999_ns", "p999"), ("latency_max_ns", "max")):
            md.append(f"**{label}**\n")
            md.append(table(fam, metric, fmt_ns))
    for fam in ("memory-uncontended", "memory-contended"):
        if not agg[fam]:
            continue
        md.append(f"#### {FAMILY_LABEL[fam]}(H=0;p50 / p99 / p999 / max)\n")
        pts = sorted(agg[fam])
        out = ["| W | p50 | p99 | p999 | max |", "|---|---|---|---|---|"]
        for (w, _h) in pts:
            vals = [agg[fam][(w, _h)].get(m) for m in
                    ("latency_p50_ns", "latency_p99_ns", "latency_p999_ns", "latency_max_ns")]
            out.append("| " + " | ".join([str(w)] + [fmt_ns(v) if v else "—" for v in vals]) + " |")
        md.append("\n".join(out) + "\n")

    md.append("### 3.3 steady_records(运行期保留记录数中位)\n")
    for fam in ("shared-uncontended", "shared-contended"):
        if agg[fam]:
            md.append(f"#### {FAMILY_LABEL[fam]}\n")
            md.append(table(fam, "steady_records", lambda v: f"{v:,.0f}"))

    md.append("### 3.4 allocs/op 摘要\n")
    md.append("| family | allocs/op 中位范围 | B/op 中位范围 |")
    md.append("|---|---|---|")
    for fam in FAMILY_ORDER:
        pts = list(agg[fam].values())
        if not pts:
            continue
        a = [p["allocs/op"] for p in pts if "allocs/op" in p]
        b = [p["B/op"] for p in pts if "B/op" in p]
        md.append(f"| {FAMILY_LABEL[fam]} | {min(a):,.0f} – {max(a):,.0f} | "
                  f"{min(b):,.0f} – {max(b):,.0f} |" if a and b else
                  f"| {FAMILY_LABEL[fam]} | — | — |")
    md.append("")

    # ---- 模型校验 ----
    md.append("## 4. 模型校验(audit §1.8:2×t_trans)\n")
    md.append("uncontended 每 cycle ≈ 2 次串行 transition,预期 ns/op ≈ "
              "2×(70µs + 3.5µs×steady_records)。偏离 >2× 或 <0.5× 标 ⚠。\n")
    md.append("| W | H | ns/op | steady_records | 预期 ns/op | 比率 | 标记 |")
    md.append("|---|---|---|---|---|---|---|")
    for fam in ("shared-uncontended",):
        for (w, h) in sorted(agg[fam]):
            p = agg[fam][(w, h)]
            if "ns/op" not in p or "steady_records" not in p:
                continue
            expected = 2 * (70_000 + 3_500 * p["steady_records"])
            ratio = p["ns/op"] / expected
            flag = "⚠" if ratio > 2 or ratio < 0.5 else ""
            md.append(f"| {w} | {h} | {fmt_ns(p['ns/op'])} | {p['steady_records']:,.0f} | "
                      f"{fmt_ns(expected)} | {ratio:.2f}× | {flag} |")
    md.append("")
    md.append("contended 深 cell 对照(实测 cycles/s;门控串行上限 1/(2×t_trans(steady));"
              "4S/W 为深队列吞吐经验式):\n")
    md.append("| W | H | cycles/s | 1/(2·t_trans) | 4S/W | N | 4W | 爬坡披露 |")
    md.append("|---|---|---|---|---|---|---|---|")
    for (w, h) in sorted(agg.get("shared-contended", {})):
        p = agg["shared-contended"][(w, h)]
        if "cycles_per_s" not in p or "steady_records" not in p:
            continue
        ceiling = 1 / (2 * (70e-6 + 3.5e-6 * p["steady_records"]))
        n = cells.get(("shared-contended", w, h, 1), {}).get("benchtime")
        ramp = "测得部分爬坡段" if (n is not None and w > CAPACITY and n < 4 * w) else ""
        md.append(f"| {w} | {h} | {fmt_count(p['cycles_per_s'])} | {ceiling:,.1f} | "
                  f"{4 * CAPACITY / w:,.3f} | {n or '—'} | {4 * w} | {ramp} |")
    md.append("")

    # ---- memory 对照 ----
    md.append("## 5. memory 对照 vs root 内部 driver(H=0)\n")
    md.append("| W | family | root cycles/s | memory cycles/s | 偏差 | root p99 | memory p99 |")
    md.append("|---|---|---|---|---|---|---|")
    for kind in ("uncontended", "contended"):
        rf, mf = f"shared-{kind}", f"memory-{kind}"
        for w in sorted({k[0] for k in agg.get(rf, {}) if k[1] == 0} &
                        {k[0] for k in agg.get(mf, {}) if k[1] == 0}):
            r = agg[rf].get((w, 0), {})
            m = agg[mf].get((w, 0), {})
            if "cycles_per_s" not in r or "cycles_per_s" not in m:
                continue
            dev = (m["cycles_per_s"] / r["cycles_per_s"] - 1) * 100
            md.append(f"| {w} | {kind} | {fmt_count(r['cycles_per_s'])} | "
                      f"{fmt_count(m['cycles_per_s'])} | {dev:+.1f}% | "
                      f"{fmt_ns(r.get('latency_p99_ns', 0))} | {fmt_ns(m.get('latency_p99_ns', 0))} |")
    md.append("")

    # ---- 环境事件 ----
    md.append("## 6. 环境事件\n")
    shown = 0
    if retry_evidence:
        md.append("**ErrCorruptState 重试存证**(单 cell 单次重试;失败 attempt 原文含 "
                  "corrupt-state dump 行):\n")
        for name in retry_evidence:
            md.append(f"- `{name}`")
            shown += 1
        md.append("")
    for fname, line in all_events:
        if shown > 200:
            md.append("- …(截断)")
            break
        if "ok " in line and fname.startswith("race-"):
            continue
        md.append(f"- `{fname}`: {line[:200]}")
        shown += 1
    if shown == 0:
        md.append("无(未见 ErrCorruptState / SKIP / panic / FAIL 事件)。\n")
    md.append("")
    md.append("**b.N 披露**:固定 Nx 下 b.N 即 N(CSV `iterations` 列);"
              "与计划不一致的 cell:\n")
    mism = 0
    for key, cell in sorted(cells.items()):
        planned = plan.get(key, {}).get("N")
        got = cell["results"][0]["metrics"].get("iterations")
        if planned is not None and got is not None and planned != got:
            md.append(f"- {key[0]} W={key[1]}/H={key[2]}/c={key[3]}: 计划 {planned}x,实测 {got:.0f}")
            mism += 1
    if mism == 0:
        md.append("- 无不一致。\n")

    # ---- 观察结论(种子)----
    md.append("## 7. 观察结论(分析员定稿)\n")
    md.append("<!-- 机械种子如下;阶段 B 由分析员复核、解释并定稿 -->\n")
    def cyc(fam, w, h):
        return agg.get(fam, {}).get((w, h), {}).get("cycles_per_s")

    for fam in ("shared-uncontended", "shared-contended"):
        pts = [(w, h) for (w, h) in sorted(agg.get(fam, {})) if h == 0]
        if len(pts) >= 2:
            w0, w1 = pts[0][0], pts[-1][0]
            a, b = cyc(fam, w0, 0), cyc(fam, w1, 0)
            if a and b:
                md.append(f"- {fam} H=0:吞吐 W={w0}→{w1} 为 {fmt_count(a)} → {fmt_count(b)} "
                          f"cycles/s({b / a:.2f}×),{'饱和' if b / a < 1.5 else '未饱和'}形态。\n")
    p999_lo = agg.get("shared-contended", {}).get((1, 1000), {}).get("latency_p999_ns")
    p999_hi = agg.get("shared-contended", {}).get((256, 1000), {}).get("latency_p999_ns")
    if p999_lo and p999_hi:
        md.append(f"- contended H=1000:p999 W=1({fmt_ns(p999_lo)})→W=256({fmt_ns(p999_hi)})"
                  f" 为 {p999_hi / p999_lo:,.0f}×(排队深度×服务时间)。\n")
    for fam in ("shared-uncontended", "shared-contended"):
        lo = agg.get(fam, {}).get((1, 0), {}).get("ns/op")
        hi = agg.get(fam, {}).get((256, 5000), {}).get("ns/op")
        if lo and hi:
            md.append(f"- {fam}:单 cycle 成本 H=0/W=1({fmt_ns(lo)})→H=5000/W=256"
                      f"({fmt_ns(hi)})为 {hi / lo:,.0f}×;模型预期 ≈ "
                      f"{(70e-6 + 3.5e-6 * 5512) / (70e-6 + 3.5e-6 * 2):,.0f}×"
                      "(tc 随 records 线性)。\n")
    for w in (1, 64, 256):
        u, c = cyc("shared-uncontended", w, 1000), cyc("shared-contended", w, 1000)
        if u and c:
            md.append(f"- W={w}/H=1000:contended({fmt_count(c)}) vs uncontended"
                      f"({fmt_count(u)})= {c / u:.3f}×。\n")
    md.append("")
    md.append(f"<!-- CSV:{args.out_csv}({len(rows)} 行)-->")

    args.out_md.write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"report: {args.out_csv} ({len(rows)} rows), {args.out_md}")
    missing = [k for k, v in plan.items() if v["outcome"].endswith("failed")]
    if missing:
        print(f"warning: plan 记录 {len(missing)} 个失败 cell", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

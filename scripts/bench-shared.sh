#!/usr/bin/env bash
# bench-shared.sh — Shared 基准矩阵执行器(task-3)。
#
# 逐 cell 执行 root 模块内部 benchDriver 矩阵(BenchmarkShared{Uncontended,
# Contended},子基准斜杠命名 W=%d/H=%d)与 driver/memory 对照矩阵
# (BenchmarkMemoryShared*,W 维度),再用 scripts/bench_shared_report.py 解析
# raw 输出为 bench-results/shared-bench.csv 与 summary.md 骨架。
#
# 硬性要求(bench-results/audit.md §4 + lead 补充条件,全部强制):
#   * bash + `set -euo pipefail`(对齐 scripts/check.sh 等的约定;pipefail 非
#     POSIX,/bin/sh 为 dash 的系统上会直接报错);任何管道都不吞 go test 退出码。
#   * 子基准正则用斜杠形式且每组件锚定,如 'BenchmarkSharedContended/W=64$/H=0$'
#     (go 按 '/' 拆分后对各组件做非锚定子串匹配:只锚定末组件不够,'W=1' 会
#     子串命中 'W=16'——实测证实;连字符形式则静默跑空)。
#   * 全部固定 -benchtime=Nx(不用时间型);contended 深 cell(W>capacity=8)
#     稳态要求 N>=4*W;预算超限时只砍 -count、绝不砍 N(砍 N 测的是爬坡段,
#     必须披露;砍 count 只损失置信度)。
#   * 每 cell 独立 go test 调用(隔离失败面),输出 tee 到
#     bench-results/raw/<family>_W<w>_H<h>_c<n>.txt,文件头含命令行+时间戳;
#     失败输出含 ErrCorruptState 的 cell 恰好重试一次,写入 <name>.retry.txt
#     (已知环境偶发:客户端墙钟后跳触发协议 fail-closed,设计行为;重试输出
#     留 corrupt-state dump 行,汇总进 summary「环境事件」节);重试仍败则记录
#     并继续其余 cell,脚本最终以非零退出码提示有失败。
#   * 每 family 显式 -timeout:contended 族 >= 2x 最坏派生 MaxWait(root 最坏
#     cell W=256×H=10000-clamped 派生 ~39min)统一 80m;go test 默认 10m 会在
#     H>=5000 深 cell 中途 panic。
#   * 续跑:raw 文件已含该 cell 计划 benchtime 下的结果行则跳过,中断后续跑。
#   * AIRLOCK_BENCH_LARGE=1 只作用于 root large 段(默认矩阵先出)。
#
# 用法:
#   scripts/bench-shared.sh [--smoke-only] [--large] [--only SECTIONS] [-h]
#
#   SECTIONS(逗号分隔,默认 smoke,root-uncontended,root-contended,memory,csv;
#   --large 在 csv 前插入 large 段;--smoke-only = race 冒烟 + 每 family 一个
#   最小 cell(W=4/H=0 @100x)+ csv,阶段 A 自测用):
#     smoke root-uncontended root-contended memory large csv
#
# 环境变量:
#   COUNT=N            常规 cell 的 count 上限(默认 3;特殊 cell 恒为 1,
#                      见 count_cap)。预算压力下降低 COUNT,不动 N。
#   BENCHTIME=Nx       覆盖所有 cell 的 benchtime(仅调试;若压低了深 cell 的
#                      N,summary 会按 N<4W 自动披露「测得部分爬坡段」)。
#   REGEX=...          调试:每模块按该 -bench 正则各跑一次(替代矩阵,
#                      benchtime 默认 100x),不产 CSV 行。
#   TIMEOUT_CONTENDED / TIMEOUT_UNCONTENDED / TIMEOUT_MEMORY / TIMEOUT_RACE
#                      各 family 的 go test -timeout(默认 80m/20m/20m/10m;
#                      contended 下限 80m = 2x ~39min 派生 MaxWait)。

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
RESULTS="$ROOT/bench-results"
RAW="$RESULTS/raw"
mkdir -p "$RAW"

COUNT=${COUNT:-3}
BENCHTIME=${BENCHTIME:-}
REGEX=${REGEX:-}
SMOKE_ONLY=0
LARGE=0
ONLY=""

usage() {
  sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case $1 in
    --smoke-only) SMOKE_ONLY=1 ;;
    --large) LARGE=1 ;;
    --only) shift; [ $# -gt 0 ] || usage 2; ONLY=$1 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown option: $1" >&2; usage 2 ;;
  esac
  shift
done

W_LIST="1 4 16 64 256"
H_LIST="0 128 1000 5000"
H_LARGE="10000"
TIMEOUT_CONTENDED=${TIMEOUT_CONTENDED:-80m}
TIMEOUT_UNCONTENDED=${TIMEOUT_UNCONTENDED:-20m}
TIMEOUT_MEMORY=${TIMEOUT_MEMORY:-20m}
TIMEOUT_RACE=${TIMEOUT_RACE:-10m}

FAILED_CELLS=""

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

plan_log() { # family W H count benchtime outcome
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6" >> "$RESULTS/plan.tsv"
}

# benchtime_for family W H — 固定 Nx 规则:
#   uncontended(全 cell)与 memory(W 维度):2000x。
#   contended 小 H(W<=64 或 H<=128):2000x。
#   contended 深队列(W=256,H>=1000):1200x(>= 4x256=1024,稳态规则)。
benchtime_for() {
  if [ -n "$BENCHTIME" ]; then printf '%s\n' "$BENCHTIME"; return 0; fi
  case $1 in
    shared-contended)
      if [ "$2" -le 64 ] || [ "$3" -le 128 ]; then echo 2000x; else echo 1200x; fi ;;
    *) echo 2000x ;;
  esac
}

# count_cap family W H — 特殊 cell 的 count 上限:
#   全部 H=10000(LARGE 段,10-18 分钟/cell)与 W=256×H>=1000 contended
#   (3/12/18 分钟/cell)用 count=1;其余为 COUNT。预算取舍:砍 count 保 N。
count_cap() {
  if [ "$3" -ge 10000 ]; then echo 1
  elif [ "$1" = shared-contended ] && [ "$2" -ge 256 ] && [ "$3" -ge 1000 ]; then echo 1
  else echo "$COUNT"; fi
}

timeout_for() {
  case $1 in
    shared-contended) echo "$TIMEOUT_CONTENDED" ;;
    shared-uncontended) echo "$TIMEOUT_UNCONTENDED" ;;
    memory-*) echo "$TIMEOUT_MEMORY" ;;
  esac
}

# cell_valid file benchtime — 文件存在、头部记录了计划 benchtime、且含结果行
# (ns/op)。失败/被杀/SKIP 的输出都没有结果行,续跑时会重跑该 cell。
cell_valid() {
  [ -f "$1" ] || return 1
  grep -q "# benchtime: $2" "$1" 2>/dev/null || return 1
  grep -q 'ns/op' "$1" 2>/dev/null || return 1
  return 0
}

# run_once out dir regex benchtime timeout large — 一次 go test 调用,
# 头部先行写入 out,输出 tee -a;返回 go test 退出码(pipefail 保真)。
run_once() {
  _out=$1; _dir=$2; _rx=$3; _bt=$4; _to=$5; _lg=$6
  {
    echo "# timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# go: $(go version)"
    echo "# cwd: $_dir"
    echo "# command: go test -run '^\$' -bench '$_rx' -benchtime $_bt -count 1 -timeout $_to"
    echo "# benchtime: $_bt"
  } > "$_out"
  _st=0
  ( cd "$_dir" && GOWORK=off \
      GOCACHE="$ROOT/.gocache/build" GOMODCACHE="$ROOT/.gocache/mod" \
      GOFLAGS=-mod=readonly AIRLOCK_BENCH_LARGE="$_lg" \
      go test -run '^$' -bench "$_rx" -benchtime "$_bt" \
        -count 1 -timeout "$_to" ) 2>&1 | tee -a "$_out" || _st=$?
  return "$_st"
}

# race_once out dir pattern — -race 冒烟;失败直接以阻塞告终(race 发现即
# blocking,不跑矩阵,报 lead)。
race_once() {
  _out=$1; _dir=$2; _pat=$3
  {
    echo "# timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# go: $(go version)"
    echo "# cwd: $_dir"
    echo "# command: go test -race -run '$_pat' -count 1 -timeout $TIMEOUT_RACE"
    echo "# benchtime: n/a (race smoke)"
  } > "$_out"
  _st=0
  ( cd "$_dir" && GOWORK=off \
      GOCACHE="$ROOT/.gocache/build" GOMODCACHE="$ROOT/.gocache/mod" \
      GOFLAGS=-mod=readonly \
      go test -race -run "$_pat" -count 1 -timeout "$TIMEOUT_RACE" ) 2>&1 | tee -a "$_out" || _st=$?
  if [ "$_st" -ne 0 ] || ! grep -q '^ok ' "$_out"; then
    log "RACE SMOKE FAILED ($_pat) — see $_out; aborting (race finding is blocking)"
    exit 1
  fi
  log "race smoke ok: $_pat"
}

# run_cell family dir benchprefix W H mode — mode: matrix|smoke|large。
# 逐 count 调用;ErrCorruptState 失败重试一次;失败记录后继续。
run_cell() {
  _fam=$1; _dir=$2; _pre=$3; _W=$4; _H=$5; _mode=$6
  _bt=$(benchtime_for "$_fam" "$_W" "$_H")
  if [ "$_mode" = smoke ]; then _bt=${BENCHTIME:-100x}; fi
  _to=$(timeout_for "$_fam")
  _lg=0; [ "$_mode" = large ] && _lg=1
  _cap=$(count_cap "$_fam" "$_W" "$_H")
  if [ "$COUNT" -lt "$_cap" ]; then _cap=$COUNT; fi
  if [ "$_cap" -lt 1 ]; then _cap=1; fi
  _rx="$_pre/W=${_W}\$/H=${_H}\$"
  case $_fam in memory-*) _rx="$_pre/W=${_W}\$" ;; esac
  _c=1
  while [ "$_c" -le "$_cap" ]; do
    _base="$RAW/${_fam}_W${_W}_H${_H}_c${_c}.txt"
    _retry="${_base}.retry.txt"
    if cell_valid "$_base" "$_bt"; then
      log "skip $_fam W=$_W H=$_H c=$_c (raw 已含 @$_bt 结果)"
      plan_log "$_fam" "$_W" "$_H" "$_c" "$_bt" reused
      _c=$((_c + 1)); continue
    fi
    if cell_valid "$_retry" "$_bt"; then
      log "skip $_fam W=$_W H=$_H c=$_c (retry 文件已含 @$_bt 结果)"
      plan_log "$_fam" "$_W" "$_H" "$_c" "$_bt" reused-retry
      _c=$((_c + 1)); continue
    fi
    _t0=$(date +%s)
    _st=0
    run_once "$_base" "$_dir" "$_rx" "$_bt" "$_to" "$_lg" || _st=$?
    _el=$(($(date +%s) - _t0))
    if [ "$_st" -eq 0 ] && cell_valid "$_base" "$_bt"; then
      log "ok $_fam W=$_W H=$_H c=$_c @$_bt ${_el}s"
      plan_log "$_fam" "$_W" "$_H" "$_c" "$_bt" "${_mode}-ran"
    elif grep -q 'ErrCorruptState' "$_base" 2>/dev/null; then
      log "RETRY $_fam W=$_W H=$_H c=$_c (ErrCorruptState, known wall-clock flake; dump 在 $_base)"
      _st2=0
      run_once "$_retry" "$_dir" "$_rx" "$_bt" "$_to" "$_lg" || _st2=$?
      _el2=$(($(date +%s) - _t0))
      if [ "$_st2" -eq 0 ] && cell_valid "$_retry" "$_bt"; then
        log "ok(retried) $_fam W=$_W H=$_H c=$_c @$_bt 共 ${_el2}s"
        plan_log "$_fam" "$_W" "$_H" "$_c" "$_bt" "${_mode}-retry-ok"
      else
        log "FAILED(after retry) $_fam W=$_W H=$_H c=$_c — see $_base / $_retry"
        FAILED_CELLS="$FAILED_CELLS $_fam/W$_W/H$_H/c$_c"
        plan_log "$_fam" "$_W" "$_H" "$_c" "$_bt" "${_mode}-retry-failed"
      fi
    else
      log "FAILED $_fam W=$_W H=$_H c=$_c exit=$_st — see $_base"
      FAILED_CELLS="$FAILED_CELLS $_fam/W$_W/H$_H/c$_c"
      plan_log "$_fam" "$_W" "$_H" "$_c" "$_bt" "${_mode}-failed"
    fi
    _c=$((_c + 1))
  done
}

section_enabled() {
  if [ "$SMOKE_ONLY" -eq 1 ]; then
    case $1 in smoke|csv) return 0 ;; *) return 1 ;; esac
  fi
  if [ -z "$ONLY" ]; then return 0; fi
  case ",$ONLY," in *",$1,"*) return 0 ;; *) return 1 ;; esac
}

write_env() {
  {
    echo "- date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- go version: $(go version)"
    echo "- uname -sm: $(uname -sm)"
    echo "- cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
    echo "- ncpu: $(sysctl -n hw.ncpu 2>/dev/null || true)"
    echo "- load: $(uptime 2>/dev/null || true)"
    echo "- params: COUNT=$COUNT BENCHTIME=${BENCHTIME:-<builtin>} REGEX=${REGEX:-<none>} LARGE=$LARGE ONLY=${ONLY:-<default>} SMOKE_ONLY=$SMOKE_ONLY"
    echo "- timeouts: contended=$TIMEOUT_CONTENDED uncontended=$TIMEOUT_UNCONTENDED memory=$TIMEOUT_MEMORY race=$TIMEOUT_RACE"
    echo "- gocache: \$worktree/.gocache/{build,mod}; GOWORK=off; GOFLAGS=-mod=readonly"
  } > "$RESULTS/run-env.txt"
}

# ---------------------------------------------------------------- debug 模式
if [ -n "$REGEX" ]; then
  log "debug REGEX mode: '$REGEX' (benchtime ${BENCHTIME:-100x}, 不进 CSV/plan)"
  _bt=${BENCHTIME:-100x}
  _st=0
  run_once "$RAW/debug_root.txt" "$ROOT" "$REGEX" "$_bt" "$TIMEOUT_CONTENDED" 0 || _st=$?
  run_once "$RAW/debug_memory.txt" "$ROOT/driver/memory" "$REGEX" "$_bt" "$TIMEOUT_MEMORY" 0 || _st=$?
  if [ "$_st" -ne 0 ]; then log "debug run non-zero"; exit 1; fi
  log "debug done"
  exit 0
fi

# ------------------------------------------------------------------- 开始
write_env
log "bench-shared start: COUNT=$COUNT sections=$( [ "$SMOKE_ONLY" -eq 1 ] && echo 'smoke,csv' || echo "${ONLY:-default}" ) large=$LARGE"

if section_enabled smoke; then
  log "== race 冒烟段 =="
  race_once "$RAW/race-root.txt" "$ROOT" \
    'TestSharedBenchmarkSmoke|TestSharedBenchBackwardClockFailsClosed'
  race_once "$RAW/race-memory.txt" "$ROOT/driver/memory" \
    'TestMemorySharedBenchmarkSmoke'
fi

if [ "$SMOKE_ONLY" -eq 1 ]; then
  log "== 冒烟最小 cell(每 family W=4/H=0 @100x)=="
  run_cell shared-uncontended "$ROOT" BenchmarkSharedUncontended 4 0 smoke
  run_cell shared-contended "$ROOT" BenchmarkSharedContended 4 0 smoke
  run_cell memory-uncontended "$ROOT/driver/memory" BenchmarkMemorySharedUncontended 4 0 smoke
  run_cell memory-contended "$ROOT/driver/memory" BenchmarkMemorySharedContended 4 0 smoke
fi

if section_enabled root-uncontended; then
  log "== root BenchmarkSharedUncontended 全矩阵 =="
  for W in $W_LIST; do
    for H in $H_LIST; do
      run_cell shared-uncontended "$ROOT" BenchmarkSharedUncontended "$W" "$H" matrix
    done
  done
fi

if section_enabled root-contended; then
  log "== root BenchmarkSharedContended 全矩阵(capacity=8)=="
  for W in $W_LIST; do
    for H in $H_LIST; do
      run_cell shared-contended "$ROOT" BenchmarkSharedContended "$W" "$H" matrix
    done
  done
fi

if section_enabled memory; then
  log "== driver/memory 对照(W 维度,H=0)=="
  for W in $W_LIST; do
    run_cell memory-uncontended "$ROOT/driver/memory" BenchmarkMemorySharedUncontended "$W" 0 matrix
  done
  for W in $W_LIST; do
    run_cell memory-contended "$ROOT/driver/memory" BenchmarkMemorySharedContended "$W" 0 matrix
  done
fi

if [ "$LARGE" -eq 1 ] && section_enabled large; then
  log "== LARGE 附加段(AIRLOCK_BENCH_LARGE=1,H=10000 种子钳制;默认矩阵已在 raw/)=="
  for W in $W_LIST; do
    run_cell shared-uncontended "$ROOT" BenchmarkSharedUncontended "$W" "$H_LARGE" large
  done
  for W in $W_LIST; do
    run_cell shared-contended "$ROOT" BenchmarkSharedContended "$W" "$H_LARGE" large
  done
fi

if section_enabled csv; then
  log "== 解析 raw → CSV + summary 骨架 =="
  python3 "$ROOT/scripts/bench_shared_report.py" \
    --raw-dir "$RAW" \
    --plan "$RESULTS/plan.tsv" \
    --env-file "$RESULTS/run-env.txt" \
    --out-csv "$RESULTS/shared-bench.csv" \
    --out-md "$RESULTS/summary.md"
fi

log "bench-shared done"
if [ -n "$FAILED_CELLS" ]; then
  echo "FAILED cells (详见 raw/ 与 plan.tsv):$FAILED_CELLS" >&2
  exit 1
fi
exit 0

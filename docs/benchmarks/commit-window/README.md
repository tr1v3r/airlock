# CommitWindow traversal benchmark / CommitWindow 遍历基准

CommitWindow record boundaries are collected during the existing configuration
restoration and final record cleanup loops. The comparison preserves the earlier
split-pass algorithm in test code, independently of the production collector.

CommitWindow 的记录边界收集合并进已有的配置恢复和最终整理循环；测试保留优化前的
独立双扫描算法作对照。公开接口、边界计算公式、原始时间精度及零窗口语义不变。

## Method / 方法

- Go 1.27.1, darwin/arm64, Apple M3 Pro, `-cpu=1`, no race instrumentation.
- Five samples per case, 200ms target per sample; table values are medians.
  Raw output is saved in [raw.txt](raw.txt).
- `splitWindowTransition` preserves boundary collection from core revision
  `3b2722fbfe94ecfe2e4569e0b047a387291e5c13`. Both variants share the state-machine
  engine: `split` disables fused collection and performs the old outer scans;
  `fused` collects within existing passes. This compares collection strategies in
  one binary, rather than benchmarking two separately checked-out revisions.
- Every fixture has one granted holder, R/2 queued requests in one tenant, and
  remaining terminal records. Lease, queue and retention durations are one hour;
  the operation time is 123 microseconds after the snapshot time. Expiry and GC
  do not run in these timed workloads. Observe targets a queued request; Renew
  targets the holder.
- The benchmark measures complete structured Transition calls: validation,
  scheduler restoration, command handling, queue capture and window collection.
  Fixture construction, JSON and backend I/O are excluded. Each call starts with
  the same snapshot header; the target record is reset in O(1) and retained
  history does not grow. Fixture queue maps remain unchanged because capture
  replaces them only on the output header.
- Correctness tests compare state, Result, errors and the exact CommitWindow on
  independent inputs. Deterministic lifecycle cases and existing model/fuzz
  traces include business errors, nil output, microsecond boundaries, non-UTC
  input, in-place renewal, protected queues and history collection.

每种规模运行五次，取中位数；性能测量在正确性检查结束后单独执行。`split` 使用冻结的
旧边界公式，在同一状态机外围扫描；`fused` 在已有循环内收集。这是同一二进制内两种
收集策略的比较，并非两个 Git 版本的独立部署性能比较。

R 包含一条持有记录、R/2 条排队记录和其余终态记录。计时包含完整结构化状态转换，
不包含构造数据、JSON 编解码或后端 I/O，也不触发过期/回收。这里的 Observe 指核心
Transition；Redis 的普通只读查询不经过该路径。每次调用复用相同输入头并用常数成本
重置目标记录，不随迭代增长历史。

## Recorded medians / 中位数记录

| Records / 记录数 | Command / 命令 | split ns/op | fused ns/op | Time change / 耗时变化 |
| ---: | --- | ---: | ---: | ---: |
| 8 | `Observe` | 5576 | 5457 | -2.13% |
| 8 | `Renew` | 5603 | 5434 | -3.02% |
| 128 | `Observe` | 73042 | 69792 | -4.45% |
| 128 | `Renew` | 72512 | 70045 | -3.40% |
| 1000 | `Observe` | 590370 | 567625 | -3.85% |
| 1000 | `Renew` | 595979 | 569351 | -4.47% |
| 10000 | `Observe` | 6230156 | 6066585 | -2.63% |
| 10000 | `Renew` | 6199312 | 5977794 | -3.57% |

Allocation counts are unchanged in this run: 80, 816, 6057 and 60107 allocs/op for
R=8/128/1000/10000 respectively, for both operations and strategies. Reported B/op
is equal or differs by at most one byte between medians; raw samples retain the
rounding variation.

本次分配次数不变，各规模分别为 80/816/6057/60107 allocs/op；两种策略的 B/op 中位数
相同或相差一字节，保留原始输出中的统计取整差异。

The two extra map scans are removed; overall transition work remains O(R).
These short local samples show a modest reduction in elapsed time. They do not
establish statistical significance, production throughput, tail latency or
Redis/PostgreSQL end-to-end speedups. Codec and storage work can dilute the gain.

这次消除了窗口计算额外的两遍 map 扫描，整体转换仍是 O(R)。本机短时样本显示有限的
耗时下降，不能据此宣称统计显著性、生产吞吐、尾延迟或后端整体收益；编码与存储成本
可能进一步摊薄收益。

## Reproduce / 复现

Run from the repository root, without other test or benchmark workloads:

```sh
GOWORK=off go test -run '^$' -bench '^BenchmarkCommitWindowTransition$' \
  -benchmem -benchtime=200ms -count=5 -cpu=1

GOWORK=off go test -run 'TestCommitWindowFusedEquivalence|TestTransitionModelTraces|TestOptimistic'
GOWORK=off go test -run '^$' -fuzz '^FuzzOptimisticWindow$' -fuzztime=5s -parallel=2
GOWORK=off go test -run '^$' -fuzz '^FuzzTransitionCommandSequence$' -fuzztime=5s -parallel=2
```

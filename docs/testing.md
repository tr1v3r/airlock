# Testing matrix and replay guide / 测试矩阵与复现

Tests establish the stated invariants within their fault models. They do not prove
arbitrary partition safety, provide fencing for external writes, or establish a
latency/throughput SLA. A green coverage percentage is not a correctness proof.

测试只验证各自故障模型下的不变量，不证明任意分区下的安全性，不为外部写入提供 fencing，
也不是吞吐或延迟 SLA。覆盖率达到阈值不等于正确性证明。

## Module boundaries

Root `go test ./...` **does not include** the nested driver modules. All commands
below use `GOWORK=off`. Each driver tests the core version pinned in its own
manifest, not the working checkout's core. A temporary development workspace can
help debug unpublished changes, but is not evidence that published driver pins
contain those changes. Push the core candidate and advance driver pins before
claiming independent-module validation.

根模块与三个 driver 必须分别验证；本地 workspace/replace 通过不代表已发布依赖图通过。
先推送 core 候选，再更新 driver pins 并独立验证，不能把旧 pin 的绿灯算作新 core 验证。

| Lane | Scope and assertions | Trigger / budget |
| --- | --- | --- |
| Compatibility | Root + memory/PostgreSQL/Redis separately: shuffled tests and vet; root local examples | Every PR/push; Linux Go 1.26/oldstable/stable, macOS/Windows stable; no automatic toolchain upgrade |
| Quality | Formatting all tracked Go; root module/dependency guard, root lint, workflow syntax; Python regression tests | Every PR/push; 15-minute job limit |
| Race / coverage | Full root suite once with uncached race and per-file/total 95% statements; every serial model and differential scenario retained | Every PR/push; 15-minute job; excludes examples/comparison/conformance helper from coverage |
| Independent drivers | `driver-check.sh`: tidy diff, verify, vet, race and per-file coverage per module; memory hello/lifecycle executable examples | Every PR/push; stable Linux; PostgreSQL ExampleNew is compile-only (no Output assertion) |
| Backend integration | PostgreSQL 16/18, Redis 7/8; shared conformance plus backend-specific locking, response-loss, corruption and cancellation cases | Every PR/push; isolated Linux services; no services needed for ordinary unit tests |
| Topology / failover | Three-primary Redis Cluster; PostgreSQL fenced synchronous standby; expected Redis asynchronous acknowledged-state loss | Every PR/push; isolated Docker fixtures; not arbitrary failover/partition proofs |
| Bounded stress | Selected Local scheduling/model/race and Shared lifecycle/clock cases once at CPU 1/2/4; four fuzz targets | Every PR/push; quick script below; no `AIRLOCK_STRESS=1` expansion |
| Extended stress | Selected normal concurrency cases ten times at CPU 1/2/4; larger Local/Shared and serial model once; repeated nested races and longer fuzz sessions | Weekly schedule or manual dispatch only; 30-minute job limit |
| Security | `govulncheck -test ./...` separately for root and each nested dependency graph | Every PR/push and weekly; Dependabot lists all four module directories |
| Installation | Fresh consumer installs core + three drivers at documented `@dev`, then at the pushed candidate; checks selected versions and compiles/runs local + memory APIs | Same-repository PRs, push, schedule/dispatch; no replace; no backend service |
| Fixture ownership | Fake Docker + fake Go: ownership and failure-order assertions without a daemon | Python suite on every PR/push; 22 fixture scenarios |

The installation lane is **explicitly skipped** for fork PRs and merge-group
synthetic commits: those SHAs need not be module-resolvable in the upstream
repository. Such runs are not published-installation evidence. CI Gate permits
only this event-specific skip and the extended lane's skip outside schedule/manual
runs. Failure/cancellation is never converted into success. Changes must be tested
at a reachable upstream candidate before publication.

安装任务只在 fork PR / merge-group 临时提交时按条件跳过；这些运行不构成发布安装验证。
其他必需任务不能跳过，失败与取消不能变绿。扩展任务只在每周/手动运行；对应 skip 条件由
Python 回归测试覆盖。每周定时任务使用仓库默认分支的工作流。

The full race/coverage run remains the safety net for every root test, fuzz seed,
and example. Only `scripts/stress.sh` owns the explicit repeated concurrency
inventory: local fairness/weighted/priority/resize/TryAcquire races, Local models
that coordinate waiter goroutines, and Shared queue/renew/release/gate/clock
regressions. The serial Transition model, partial-state differential tests, scale
fixtures and benchmark smoke tests are not repeated for CPU scheduling. New
concurrency regressions should be added to that inventory; no test is removed
from the full suite. Verbose race/stress logs expose cases, elapsed times and
shuffle seeds instead of remaining silent until a package completes.

全量 race/覆盖率仍执行根模块全部测试、fuzz 种子和示例；只有选定并发用例进入 CPU
重复矩阵。串行 Transition 模型、部分状态差分、规模 fixture 和 benchmark smoke
不做调度重复。新增并发回归应同步加入 `scripts/stress.sh` 的清单；全量测试不删减。
详细日志输出用例、耗时与乱序种子。

### Uniform Go 1.26 floor

Every module requires Go 1.26. The PostgreSQL driver set the previous Go 1.25
floor: pgx v5.9.2 and x/text v0.41.0 (the versions fixing GO-2026-5004 SQL
injection and the x/text advisories) both declare `go 1.25.0`. The floor then
moved to 1.26 with the dependency updates go-redis v9.23.0 and
golang.org/x/sync v0.24.0, which both declare `go 1.26.0`; retaining the
older versions to preserve Go 1.25 was not an acceptable compatibility
workaround. Core, memory and Redis adopt the same minimum deliberately:
one toolchain requirement, one CI matrix entry per supported line, and no
module-specific skips. `GOTOOLCHAIN=local` in CI keeps the 1.26.x entry honest
as the minimum-version lane; older toolchains are no longer supported anywhere.

全部模块统一要求 Go 1.26。Go 1.25 下限由 PostgreSQL 率先确立：修复
GO-2026-5004 SQL 注入与 x/text 公告的 pgx v5.9.2、x/text v0.41.0 均声明
`go 1.25.0`；go-redis v9.23.0 与 golang.org/x/sync v0.24.0（均声明
`go 1.26.0`）将下限推至 1.26，为保留 Go 1.25 而停在旧版本不是可接受的
兼容性方案。core、memory、Redis 有意采用同一最低版本：单一工具链要求、
单一 CI 矩阵、无任何模块跳过。CI 的 `GOTOOLCHAIN=local` 保证 1.26.x 条目
真实验证最低版本；更旧的工具链在全仓库不再受支持。

## Reproducible commands

Run from the repository root unless a command explicitly changes directory:

```sh
# No services: ordinary tests and the checked-in deterministic/fuzz seed corpus.
GOWORK=off go test -race -shuffle=on -count=1 -timeout=5m ./...
bash scripts/driver-check.sh
python3 -m unittest discover -s scripts -p 'test_*.py' -v

# Normal PR budget: selected concurrent cases once at CPU 1/2/4;
# each fuzz target 5s / two workers. Full coverage already runs the normal serial
# model (8 seeds x 128 steps) and all partial-state differential cases once.
bash scripts/stress.sh quick

# Larger opt-in workload: selected normal concurrent cases at CPU 1/2/4 x ten
# repetitions; then AIRLOCK_STRESS=1 Local/Shared once at CPU 1 (20-minute bound),
# serial model once at 64 seeds x 250 steps; nested tests at CPU 1/2/4 x three
# repetitions; each fuzz target 30s / two workers.
# Not part of the normal PR critical path.
bash scripts/stress.sh extended

# Isolate a failing model/concurrency case; preserve the printed shuffle/trace seed.
GOWORK=off go test -race -run '^TestLocal' -count=1 -shuffle=12345 .
GOWORK=off go test -race -run '^TestShared(Concurrent|Clock)' -count=1 .
GOWORK=off go test -run '^TestTransitionModelTraces$' -count=1 -v .

# Fuzz one target independently; minimized failures are replayed by ordinary tests.
GOWORK=off go test -run '^$' -fuzz '^FuzzLocalDynamicModel$' -fuzztime=30s -fuzzminimizetime=1s -parallel=2 .
GOWORK=off go test -run '^$' -fuzz '^FuzzTransitionSnapshot$' -fuzztime=30s -fuzzminimizetime=1s -parallel=2 .
GOWORK=off go test -run '^$' -fuzz '^FuzzTransitionCommandSequence$' -fuzztime=30s -fuzzminimizetime=1s -parallel=2 .
GOWORK=off go test -run '^$' -fuzz '^FuzzOptimisticWindow$' -fuzztime=30s -fuzzminimizetime=1s -parallel=2 .

# Security must visit each manifest; root's ./... cannot substitute for this loop.
for module in . driver/memory driver/postgres driver/redis; do
  (cd "$module" && GOWORK=off govulncheck -test ./...)
done

# Requires a pushed core/driver candidate, not an unpublished local commit.
bash scripts/install-smoke.sh <pushed-commit>
```

The install wrapper first checks both READMEs' literal `@dev` instruction (it never
executes Markdown). The documented lane follows their **separate** commands: core
first, then each driver. Module proxies cache mutable branch queries independently;
core `@dev` can lag a driver's `@dev`. Combining all four into a single `go get` can
then demand an older exact core version and conflict with the driver's requirement.
Separate commands let Go's MVS upgrade core to satisfy the selected drivers.

The documented lane therefore records the selected `go list -m -json all` graph,
compiles/runs it with `-mod=readonly`, and verifies its module checksums. Go's MVS
enforces driver requirements; this is an internally consistent dependency graph,
not a guarantee that every proxy branch query reflects the newest remote commit.
It does **not** re-query `@dev` and compare that potentially stale response to core
after a legitimate MVS upgrade. A stale version lacking required APIs still fails
compilation or execution; this is not a reason to bypass the smoke check.

The candidate lane installs immutable commit versions together and **still requires
exact equality** between resolved and selected versions. Only that lane additionally
compiles `ClockSkewAllowance`, which need not exist on `dev` before the PR merges.
Both lanes use a fresh consumer, `GOWORK=off`, and no `replace`. Network/proxy/build
failures are failures, not silently downgraded to a workspace build.

安装脚本先核对中英文 README 的字面 `@dev` 命令，不执行 Markdown，并按文档先装 core、再
逐条安装 driver。代理分别缓存各模块的可变分支查询，core 缓存可能比 driver 旧；单条同时
指定四个模块可能要求旧 core 精确版本，导致依赖冲突。分开安装允许 Go 的 MVS 根据 driver
需求升级 core。文档 lane 记录已选择的完整图、只读编译执行并校验模块，不重新查询可能
滞后的 `@dev` 来否定合法升级；这保证图内部一致，不保证代理已经看到远端最新提交。
旧版本缺 API、网络或构建失败仍会报错。候选 lane 使用不可变提交，仍严格核对版本相等，
且独自验证新增时钟余量字段；两条 lane 均不使用 workspace/replace。

## Scale and deterministic boundaries

- Local: fixed-seed dynamic queues, weighted grants/cancellation and priority versus
  ungrouped behavior; concurrent storms, idempotent release, MaxInt arithmetic.
  `AIRLOCK_STRESS=1` enlarges workloads, not assertions or acceptable outcomes.
- Shared: concurrent acquire/renew/release/cancel and bounded client/backend clock
  offsets; lifecycle example checks cancellation while renewal blocks, join-before-
  release, and same-lease cleanup retries. The application must stop its actual work.
  Quick mode runs the selected normal concurrency cases once at CPU 1/2/4;
  extended mode repeats them ten times at each CPU count with normal fixture sizes.
  Expanded Shared concurrency and the Local storm then run once at CPU 1 with a
  20-minute bound: the 128-client/eight-round Shared lifecycle workload already
  expands concurrency internally. Nested drivers retain CPU 1/2/4 × three runs.
  quick 对选定普通规模并发用例按 CPU 1/2/4 各跑一次，extended 各重复十轮；
  扩大的 Shared 和 Local storm 随后单遍执行（CPU=1、20 分钟上限），不把扩展规模
  乘重复矩阵。nested driver 保留 CPU 1/2/4 各三轮。
- Durable protocol: deterministic operation traces, malformed snapshots/commands,
  publication-window boundary fuzzing and restore allocation scaling. The serial
  model runs without the CPU/repetition multiplier: full race/coverage executes
  8 seeds × 128 steps once; extended mode additionally executes 64 × 250 once,
  alternating ungrouped/priority mode by seed, with a three-minute test deadline
  for the expanded model. CPU schedules add no interleavings to this serial
  model. Fuzz inputs are bounded; this is not unbounded exhaustion testing.
  串行状态模型不参与并发 CPU/重复乘积；全量 race/覆盖率执行普通 8×128，
  extended 另执行一次扩展 64×250，均覆盖无分组/优先级两种模式。
- Driver conformance: concurrent same-identity calls, owner conflicts, cancel versus
  submit, renew versus release, and pre-cancelled operations. Normal fanout is 8;
  stress fanout is 16 where configured. These shared helpers are in the pinned core.
- Real backends and fake backends complement each other. Mocks cannot establish
  socket, SQL lock, Redis Cluster or replication behavior; real integration tests
  do not simulate every possible network partition or process scheduling pause.

Benchmarks are opt-in evidence, not timing assertions on shared CI hardware:

```sh
GOWORK=off go test -run '^$' -bench '^BenchmarkTransition(Optimistic)?Scale$' -benchmem -count=3 .
# Limit the large matrix explicitly; R is records, M tenant mappings, Q queued records.
AIRLOCK_BENCH_LARGE=1 GOWORK=off go test -run '^$' \
  -bench '^BenchmarkTransitionScale$/^R=1000$/^M=(0|1000)$/^Q=0$' -benchmem -benchtime=3x .
```

### Shared client benchmark matrix / Shared 客户端基准矩阵

`shared_benchmark_test.go` measures single-token AcquireFor/Release cycles
across workers W, retained terminal seeds H, and uncontended/contended families.
Acquire latency excludes Release; cycle throughput includes it. The internal
driver follows memory.Driver's per-resource algorithm: cached Observation reads
between maintenance boundaries; complete decode/Transition/encode for writes
and due maintenance. It retains the resource gate even on fast reads.
The black-box `driver/memory` comparison starts with H=0. Root omits the real
driver's resource-lookup mutex, and the nested module uses its pinned core;
inspect `go list -m all` in each module before comparing results. Equal costs
are not an invariant. These are not PostgreSQL/Redis performance measurements.

Run `bash scripts/bench-shared.sh --smoke-only` for race smoke and small cells,
or `bash scripts/bench-shared.sh` for the full matrix. The full matrix is
opt-in; normal CI runs ordinary harness tests, not all benchmark cells.
Use fixed `-benchtime=Nx`; contended cells require at least `N >= 4*W` by the
sampling-budget rule. This avoids very short startup-only runs but does not
prove a steady queue depth. The W-squared deadline allowance is a conservative
historical safety budget, not a performance prediction for read-only Observe.
Raw output, CSV and summaries live in ignored `bench-results/`. Resume only
accepts results with the current source fingerprint and requested iteration count;
old full-transition polling measurements must be regenerated. A clock-injection
test proves that backward time fails closed; it does not establish the cause
of every ErrCorruptState. Failed outputs remain available after any retry.

根模块矩阵按 W（并发 worker）、H（保留的终态种子记录）和无争用/争用场景，
测量单 token 的 AcquireFor/Release 周期。Acquire 延迟不含 Release，周期吞吐包含。
内部驱动与 memory.Driver 使用相同的资源内算法：免维护期读取缓存 Observation；
写入和到期维护才完整解码、转换、编码。快路径仍持有资源 gate。
真实 memory 驱动对照从 H=0 开始；根模块省略资源查找 mutex，嵌套模块还使用自己
固定的 core 版本，比较前分别查看 `go list -m all`，不能要求两者耗时相等。
这套矩阵不代表 PostgreSQL/Redis 的实际性能。

`--smoke-only` 执行 race 冒烟和小规模基准，完整矩阵由使用者主动运行；普通 CI
只执行相关测试。争用场景使用固定 Nx，按样本预算规则要求 N 至少为 4W；这能避免
仅测极短启动阶段，但不保证队列已达到稳态。W² 等待窗口保留为保守安全预算，
不能沿用为只读 Observe 的性能预测。输出保存在忽略的 `bench-results/`；续跑仅
复用源码指纹和迭代数匹配的结果，旧的全量 Observe 数据必须重跑。时钟注入测试
只证明时间倒退会失败关闭，不把所有 ErrCorruptState 都归因于时钟；重试保留失败输出。

## Isolated service fixtures and ownership

See [module commands](../MODULES.md) for real-backend environment variables.
Use dedicated test services: Redis integration flushes the script cache;
PostgreSQL integration creates schema/unique resource rows. Do not aim these tests
at a production endpoint.

The three Docker scripts retain a network/container ID **only after successful
creation**, and all cleanup uses those IDs rather than proposed names. Fake tests
exercise network conflicts, first/second/third container conflicts, partial success
followed by a failing query, test failure, TERM interruption, and normal exit.
They also model failure stdout to ensure it cannot be mistaken for an owned ID.
SIGKILL, host crashes and a Docker create response lost after creation can leave
orphan fixtures; the scripts do not delete guessed names to hide those cases.

三个脚本只记录成功创建后的资源 ID，清理不用候选名称，因此冲突不会误删他人的资源。
fake 测试覆盖冲突、部分成功后失败、测试失败、TERM 与正常退出，包括失败命令仍输出文本的
情形。SIGKILL、宿主崩溃、创建成功但响应丢失可能遗留 fixture；不能为消除遗留而猜名称删除。

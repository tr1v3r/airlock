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
| Compatibility | Root + memory/PostgreSQL/Redis separately: shuffled tests and vet; root local examples | Every PR/push; Linux Go 1.24/1.25/oldstable/stable, macOS/Windows stable; PostgreSQL explicitly excluded only on 1.24 (minimum 1.25); no automatic toolchain upgrade |
| Quality | Formatting all tracked Go; root module/dependency guard, root lint, workflow syntax; Python regression tests | Every PR/push; 15-minute job limit |
| Race / coverage | Root uncached race, CPU 1/2/4 × ten repetitions, per-file and total 95% statements | Every PR/push; 15-minute job; excludes examples/comparison/conformance helper from coverage |
| Independent drivers | `driver-check.sh`: tidy diff, verify, vet, race and per-file coverage per module; memory hello/lifecycle executable examples | Every PR/push; stable Linux; PostgreSQL ExampleNew is compile-only (no Output assertion) |
| Backend integration | PostgreSQL 16/18, Redis 7/8; shared conformance plus backend-specific locking, response-loss, corruption and cancellation cases | Every PR/push; isolated Linux services; no services needed for ordinary unit tests |
| Topology / failover | Three-primary Redis Cluster; PostgreSQL fenced synchronous standby; expected Redis asynchronous acknowledged-state loss | Every PR/push; isolated Docker fixtures; not arbitrary failover/partition proofs |
| Bounded stress | Local dynamic model/storm, Shared concurrent/clock tests, durable model traces; four fuzz targets | Every PR/push; quick script below; no `AIRLOCK_STRESS=1` expansion |
| Extended stress | Larger local/Shared/model workloads; repeated nested races and longer fuzz sessions | Weekly schedule or manual dispatch only; 30-minute job limit |
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

### Intentional PostgreSQL toolchain boundary

Core, memory and Redis retain Go 1.24 support. PostgreSQL requires Go 1.25:
pgx v5.9.2 and x/text v0.41.0 both require it. The pgx upgrade addresses
GO-2026-5004 (SQL injection); retaining the vulnerable version merely to preserve
Go 1.24 would not be an acceptable compatibility workaround. CI prints the reason
and skips **only** PostgreSQL in the 1.24 matrix entry, while testing memory and
Redis there as usual. PostgreSQL remains covered on 1.25 and newer matrix entries,
and by the stable-toolchain race, security and real-backend jobs. CI Gate needs no
extra skip exception because the compatibility job itself still runs and succeeds.

core、memory、Redis 保持 Go 1.24；PostgreSQL 因 pgx SQL 注入安全修复升级依赖，
最低版本有意提高到 Go 1.25。1.24 矩阵只跳过 PostgreSQL 并打印原因，不跳过另外两个
模块或整个 job；PostgreSQL 在 1.25 及更新矩阵、race、安全扫描和真实后端任务继续验证。
Python 回归执行实际 workflow 中的循环，断言各版本测试的模块集合，防止静默漏测。

## Reproducible commands

Run from the repository root unless a command explicitly changes directory:

```sh
# No services: ordinary tests and the checked-in deterministic/fuzz seed corpus.
GOWORK=off go test -race -shuffle=on -count=1 -timeout=5m ./...
bash scripts/driver-check.sh
python3 -m unittest discover -s scripts -p 'test_*.py' -v

# Normal PR budget: concurrent cases at CPU 1/4, one serial 8-seed × 128-step model;
# each fuzz target 5s / two workers.
bash scripts/stress.sh quick

# Larger opt-in workload: AIRLOCK_STRESS=1; Local/clock and nested tests at
# CPU 1/2/4 × three repetitions; heavy Shared once at CPU 1 (20-minute bound);
# serial model once at 64 seeds × 250 steps; each fuzz target 30s / two workers.
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
  In extended mode, `TestSharedConcurrent*` runs once at CPU 1 with a 20-minute
  bound: the 128-client/eight-round lifecycle workload already expands concurrency
  internally. Local/clock and nested driver tests retain CPU 1/2/4 × three runs;
  quick mode's combined Local/Shared/clock schedule remains unchanged.
  扩展 Shared 高并发组单遍执行（CPU=1、20 分钟上限），不再把 128 客户端×8 轮乘 9；
  Local/clock 与 nested driver 保持 CPU/重复矩阵，普通 quick 任务不改。
- Durable protocol: deterministic operation traces, malformed snapshots/commands,
  publication-window boundary fuzzing and restore allocation scaling. The serial
  model runs once, without the CPU/repetition multiplier: 8 seeds × 128 steps in
  quick mode, 64 × 250 in extended mode, alternating ungrouped/priority mode by
  seed, with a three-minute test deadline. CPU schedules add no interleavings to this serial
  model. Fuzz inputs are bounded; this is not unbounded exhaustion testing.
  串行状态模型不参与并发 CPU/重复乘积；quick 为 8×128，extended 为 64×250，
  均覆盖无分组/优先级两种模式。Local 保留 CPU/重复矩阵，Shared 的扩展规模按上文单独设限。
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

# 活动状态与终态历史分离：前后对照

本报告比较基线 `0a599bf` 与实现提交 `3dda021`，后续仅补报告和复现脚本说明。
实验在隔离的真实 Redis 8 / PostgreSQL 18 上执行，使用相同混合负载源码和参数。
这些本地闭环样本用于评估本次修改，不是生产 SLO 或后端最大容量。

## 实现与语义

普通操作不再载入无关的未到期终态。Redis 将 active snapshot 与终态字段／同槽到期索引分开；
PostgreSQL 保持同表，通过活动索引和目标主键读取，并维护精确计数与终态到期索引。
活动状态、目标终态、到期维护、计数和调度结果在原资源锁与提交边界内更新。
完整 `Transition` 及 memory/custom-driver 路径保持兼容，分离后端使用 `TransitionPartial`。

保留的语义包括：请求参数冲突先于 GC、完整 owner／leaseID 校验、
`max(Finished+Retention, SubmitUntil)` 回收窗口、精确 MaxRecords、公平调度和 protected head、
容量及 Resize(0)、Redis owner/CommitWindow，以及不自动重放未知结果的 mutation。
迁移及部署限制见[双语协议指南](../../guides/partitioned-history.zh-CN.md)。

v1 首次提交会完整验证并迁移，旧 writer 不能写 v2。Redis 多命令发布错误会保持 incomplete、拒绝后续授权，
不自动重建空资源。PG 同事务迁移、回收和更新。未访问终态不再每次全量复验，应禁止绕过驱动改写存储。
本步仍一次处理全部已到期历史，成本可随到期量上升，也仍需恢复全部活动调度状态。

## 测试方法

6 场景 × 2 后端 × 3 轮 × 2 版本，共 72 次独立试验，每次 15 秒。
每个 worker 循环 Submit → 排队时每 50ms Observe → 获准后持有 600ms、每 200ms Renew → Release。
TTL=2s，单操作超时500ms，weight=1，8个租户，连接池上限max(workers,16)。
H/C/W 分别为初始终态数、容量、worker 数：0/8/8、0/8/32、1000/8/32、10000/8/32、1000/32/64，
另有 1000/32/32 的集中到期场景，预置额外32个持有者与32个排队者，约2s时统一到期并回收初始历史。

同一后端／场景内交错两个版本，第一、三轮先基线，第二轮先候选，并改变场景和后端顺序。
两版本分别编译同一份 `tools/mixedload/main.go`，基线 worktree 仅覆盖这份测试工具，核心与驱动仍来自 `0a599bf`。
工具只增加新旧存储统计兼容和不改变行为的 lint 整理；计时内操作、频率、阈值、失败清理策略均相同。
每次使用独立资源／PG表。初始化、预热迁移和完整前后统计不计入15s。
因此结果比较的是迁移后稳态，不代表首次升级成本；SetupSeconds 还包含建表、seed、统计，不能当作单独迁移耗时。

环境与镜像ID见 [environment.json](environment.json)，二进制构建来源见 baseline-build.txt / candidate-build.txt。
正式测量期间团队暂停其他编译、测试与压力任务；没有消除所有 OS／虚拟机背景活动。
运行时与请求ID具有正常随机性，短窗口及闭环模型仍有调度、缓存、checkpoint等波动。

## 指标口径

- 准入/s：窗口内交付且保守预算可用的业务 grant，不包含预置 seed；600ms持有约束理想上限在C/0.6附近。
- 操作/s：窗口内完成的Submit/Observe/Renew/Release等操作，包含错误，不等于业务吞吐。
- 错误率：窗口内返回错误的操作数／操作总数。结束边界和cleanup单列，超时不能证明后端未提交。
- 延迟：all/ok/error 分层；Renew all 包含超时。每轮p95至少20样本、p99至少100样本，报告轮间范围，不混成单个总体分位数。
- 续租预算：Remaining减完整RTT的保守本地估算；不是精确后端TTL。未确认响应无法由成功样本证明安全。
- 累计Go分配：从计时开始到收尾完成，包含工具样本缓冲及cleanup；heap每100ms采样，包含客户端及fixture，不是后端RSS。
- 后端bytes：Redis包含主hash与终态ZSET；PG包含表和索引关系大小。二者含义不同，不能跨后端直接比较。
- 终态数、seed保留／集中回收、已确认grant预算与末态容量做不变量检查；末态检查不证明全过程无超发。

## 复现与数据

使用独占、一次性服务。保存的 [runs/](runs/) 包含两个版本的每次压缩逐调用JSONL、summary和stderr；失败样本不剔除。
`compare.py` 拒绝非空输出目录，任一进程失败即停止并保留已写数据。分析器拒绝重复／缺失或计数不一致的样本。

```sh
# 在候选仓库根目录，创建单独的对照checkout；名称必须尚未使用。
git worktree add --detach .worktrees/terminal-before 0a599bf
cp tools/mixedload/main.go .worktrees/terminal-before/tools/mixedload/main.go
(cd .worktrees/terminal-before/tools/mixedload && GOWORK=off go build -o /tmp/airlock-terminal-baseline .)
(cd tools/mixedload && GOWORK=off go build -o /tmp/airlock-terminal-candidate .)
# 输出目录必须不存在或为空。
REDIS_ADDR=127.0.0.1:PORT POSTGRES_DSN='postgres://USER:PASS@127.0.0.1:PORT/DB?sslmode=disable' \
  python3 tools/mixedload/compare.py /tmp/fresh-terminal-comparison \
  --baseline /tmp/airlock-terminal-baseline --candidate /tmp/airlock-terminal-candidate
python3 tools/mixedload/compare_report.py /tmp/fresh-terminal-comparison
```

## 正确性验证

根状态机 1224 项差分子测试比较完整结果、提交窗口与重组状态；根test/vet/race及coverage通过。
驱动独立模块 tidy/verify/vet/race、逐文件95% coverage通过；真实Redis7 standalone/Cluster、Redis8、PG17/18通过。
迁移、旧writer拒绝、到期目标冲突回滚、超过64条到期回收、MaxRecords、计数／索引关联、
Redis中途脚本错误保持不可用等均有回归；交叉CR已修复nil记录及元数据／索引关联处理。
核心、两驱动与负载工具lint通过。

## 前后结果

下表为各自三轮最小–最大值；分配量是每轮累计GB。逐轮全指标见 [comparison.csv](runs/comparison.csv)，逐操作p50/p95/p99见两个版本的latency.csv。

| H/C/W（场景） | 后端 | 准入/s 基线 → 候选 | measured错误率% 基线 → 候选 | Renew all p95 ms 基线 → 候选 | 分配GB 基线 → 候选 | cleanup错误/轮 基线 → 候选 |
|---|---|---|---|---|---|---|
| 1000/8/32 | redis | 12.33–12.33 → 12.80–12.80 | 0.000–0.176 → 0.000–0.000 | 73.0–99.7 → 14.0–22.0 | 4.870–4.930 → 0.182–0.186 | 8–11 → 0–0 |
| 1000/8/32 | postgres | 12.47–12.60 → 12.80–12.80 | 0.000–0.000 → 0.000–0.000 | 33.2–36.4 → 24.5–28.0 | 1.820–1.844 → 0.254–0.262 | 0–0 → 0–0 |
| 10000/8/32 | redis | 3.47–4.07 → 12.80–12.80 | 50.822–63.209 → 0.000–0.000 | 501.1–504.0 → 15.0–18.1 | 10.207–12.251 → 0.175–0.178 | 402–422 → 0–0 |
| 10000/8/32 | postgres | 2.00–6.73 → 12.40–12.47 | 2.341–21.306 → 0.000–0.000 | 500.4–501.8 → 32.5–39.9 | 6.528–7.348 → 0.276–0.292 | 84–276 → 0–0 |
| 1000/32/64 | redis | 22.07–23.53 → 50.27–51.13 | 5.244–6.264 → 0.000–0.000 | 502.0–502.7 → 34.8–62.9 | 8.442–8.532 → 1.005–1.016 | 215–265 → 0–16 |
| 1000/32/64 | postgres | 35.00–36.20 → 48.47–49.80 | 0.217–0.286 → 0.000–0.008 | 371.0–403.9 → 61.2–151.5 | 4.512–4.627 → 0.871–0.879 | 17–20 → 0–0 |
| 1000/32/32 到期 | redis | 34.80–36.40 → 44.67–44.87 | 2.147–2.568 → 0.000–0.137 | 386.9–500.0 → 40.2–66.1 | 3.968–4.183 → 0.421–0.426 | 4–9 → 0–0 |
| 1000/32/32 到期 | postgres | 37.93–41.00 → 44.60–44.87 | 0.000–0.000 → 0.000–0.000 | 98.3–178.1 → 48.0–64.1 | 1.766–2.030 → 0.320–0.326 | 0–0 → 0–0 |

小场景不是统一的尾延迟改善：C8/W8 时，两版本准入基本相同、均无measured错误；PG候选某轮Renew p99为186.3ms，而基线三轮39.8–49.8ms。
C8/W32/H0时准入仍约12.8/s；等待Observe仍放大操作量。本修改没有改变固定50ms轮询或业务持有时间。

H10000时Redis候选三轮准入均12.8/s、PG12.40–12.47/s，两者均无measured或cleanup错误。Redis Renew all p95由501.1–504.0ms降为15.0–18.1ms；PG由500.4–501.8ms降为32.5–39.9ms。
基线Redis该场景每轮Renew不足100样本，不报告p99；PG基线也仅1/3轮达到p99样本门槛，不能以缺失尾分位数作改善比例。候选有更多已完成工作，累计分配仍明显减少。

高并发候选仍有失败：PG C32/W64其中一轮1次Submit deadline；Redis该场景measured错误为0，但两轮有12/16次cleanup错误，最后结束边界可能仍有未确认或等待TTL的记录。
集中到期候选Redis有少量measured deadline（0–0.137%），也不能声称消除了到期峰值。前后期活动与操作构成变化，整体差异不能全部归因于GC执行时间。

## 后续建议

1. 保留本次分离作为历史规模治理的第一步，同时给单资源活动队列／租约规模设运行预算；MaxRecords不是延迟保证。
2. 采集CPU/分配/锁等待profile，再评估活动状态增量维护与续租热路径。新结果支持历史处理是重要成本，但未测profile，不能把全部收益归给某一个函数或GC。
3. 测包含当前Shared轮询抖动的端到端路径、混合权重、多资源、跨机网络和自然长期历史增长。当前只测Driver，固定轮询依然存在。
4. 单独验证迁移高水位、真实持久化/复制配置和长期索引/vacuum成本；本轮不量化在线迁移耗时或长期存储增长。
5. 再考虑分批物理GC。先定义逻辑回收、准确MaxRecords及索引推进，不能因预算截断而误拒绝申请，也不能把已逻辑遗忘的旧记录再次用于请求冲突。

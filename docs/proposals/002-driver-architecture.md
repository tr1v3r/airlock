# 提案 002：本地与分布式 Driver

状态：core 契约、客户端与 memory 在当前 PR 实现，详见 [ADR 005](../decisions/005-driver-contract.md)；PostgreSQL 在后续独立 PR 实现（见 [ADR 006](../decisions/006-postgres-driver.md)），Redis 由独立模块实现（见 [ADR 007](../decisions/007-redis-driver.md)）。以下保留原设计方向，API 以当前源码为准。核对日期：2026-10-22。配套：[总提案](002-weighted-priority-drivers.md)、[调度契约](002-weighted-scheduling.md)。

## 决策

先实现进程内多租户、多令牌、优先级调度，再以独立 driver 模块提供 Redis 和 PostgreSQL。
三者遵守相同的队列与调度契约；远端增加网络错误、租约和恢复契约，不能假装等价于本地内存调用。
容量约束是同一资源中有效授权的权重总和不超过 Capacity，不能把申请 n 个令牌实现成 n 次单令牌申请。

本地 `New(Config)` 保持零外部服务、标准库运行时依赖；已有 `Permit.Release()` 保留。
远端拟提供独立 `NewShared(driver, ...)` 和 `Lease.Release(ctx) error`，使用者明确选择网络生命周期。
这是接口方向，不是已实现 API；本次不改变现有方法。

## 借鉴 anylock 的范围

只读核对相邻 anylock，基准提交 `16153457be5b39d79fae0ec2d58c9ae73a4c1948`：

| 可定位实现 | 采用的做法 | Airlock 的差异 |
| --- | --- | --- |
| [driver.go:18-60](https://github.com/tr1v3r/anylock/blob/16153457be5b39d79fae0ec2d58c9ae73a4c1948/driver.go#L18-L60) | 根包定义原子 Driver 契约，明确 owner、到期、模糊结果 | 增加持久等待队列、调度状态、请求幂等与多 holder |
| [MODULES.md](https://github.com/tr1v3r/anylock/blob/16153457be5b39d79fae0ec2d58c9ae73a4c1948/MODULES.md) | memory/redis/postgres 独立模块，integration 独立测试模块 | 本地 Airlock 仍是直接入门入口，memory driver 复用状态机 |
| [Redis driver](https://github.com/tr1v3r/anylock/blob/16153457be5b39d79fae0ec2d58c9ae73a4c1948/driver/redis/redis.go#L74-L80) | 外部注入客户端，调用者拥有连接资源 | 单个 SET NX 不足以管理加权容量与公平队列 |
| [Postgres driver](https://github.com/tr1v3r/anylock/blob/16153457be5b39d79fae0ec2d58c9ae73a4c1948/driver/postgres/postgres.go#L43-L54) | 获取行锁后检查当前到期时间；owner 校验释放 | 每个资源的队列、容量与授予必须同一事务提交 |

不照搬 `TryAcquire` 重试循环：客户端谁重试快谁拿到令牌，会破坏租户轮转、优先级配额及大请求防饿死。
也不照搬互斥锁的单全局 fencing token，详见安全边界。

建议目录：

```text
.                   # 标准库 core；本地 Airlock、调度状态机、分布式契约/客户端
internal/...        # 本地调度实现及测试；不直接暴露可变内部状态
driver/memory/     # 独立模块；进程内 Driver，用于同契约测试与可替换后端演示
driver/redis/      # 独立模块；core + go-redis
driver/postgres/   # 独立模块；core + database/sql，pgx 留在示例/测试
integration/      # 独立模块；跨模块及真实服务测试
```

memory driver 不维护第二份调度算法：通过根包提供的受控、标准库实现入口复用本地状态机。
它模拟租约及远端协议，但不能模拟真实网络分区、复制或数据库持久性。
根包测试、示例不能反向导入 driver，避免把第三方依赖带回根模块。
各模块使用真实可获取的 core 版本；开发 go.work 不替代 `GOWORK=off` 的独立消费验证。

## 权威状态与协议

每个 Resource 是独立容量池，记录不可变配置版本、Capacity、InUse、队列上限、优先级配置与状态版本。
后端保存各优先级游标、租户轮转顺序、租户 FIFO、选中但暂不能授予的请求、有效 leases。
提交入口校验配置一致性；不同客户端不能以不同容量或优先级配置操作同一个 Resource。
第一版不支持在线改容量或更改调度策略；各优先级 Turns 总和不得超过 1024。

所有后端采用同一语义。以下为历史提案草图；当前 Request、LeaseRef 和 Result 签名见
[已实现的 driver 契约](../decisions/005-driver-contract.md)。

```go
type Driver interface {
    Submit(ctx context.Context, req SubmitRequest) (RequestState, error)
    Observe(ctx context.Context, ref RequestRef) (RequestState, error)
    Cancel(ctx context.Context, ref RequestRef) (RequestState, error)
    Renew(ctx context.Context, ref LeaseRef, ttl time.Duration) (LeaseState, error)
    Release(ctx context.Context, ref LeaseRef) (LeaseState, error)
}
```

`SubmitRequest` 包含 Resource、配置版本、RequestID、随机不可猜测 Owner、TenantID、Weight、Priority、
不可变提交截止时间、最大排队时间、LeaseTTL。Priority 必须由后端不可变 TenantGroups 映射派生；
如请求显式携带 Priority，则校验与映射一致，拒绝客户端自行提权。`RequestRef` 至少携带 Resource、RequestID、Owner。
`LeaseRef` 另含该次授予的 LeaseID；禁止按租户或资源整体释放。
状态含 backend revision、终态、已授权的重量和保守剩余有效期，不能让客户端自行决定是否已授予。

- `Submit` 原子校验 `0 < Weight <= Capacity`、配置、队列上限与幂等记录，再入队/调度。
- 相同 RequestID 与相同不可变参数只返回已有状态，不新增排队、重复记账或延长 TTL；参数冲突报错。
- `Observe` 查询并帮助推进该资源的过期回收和调度。客户端轮询顺序不决定授予顺序。
- `Cancel` 原子地将 queued 移除，或将已经 granted 的授权终止并归还全部 Weight；返回权威结果。
- `Renew` 只延长仍有效且 owner/lease 匹配的授权，不能使已过期、已释放或取消的授权复活。
- `Release` 原子记录终态并仅归还一次完整 Weight；重复调用返回同一终态，不下溢。
- 错误必须保留身份。后端不可用不等于队满或竞争失败，不允许自动降级为独立本地池。

每个调用的容量变更与其调度状态迁移在一个原子边界内；回收后才能按统一规则授予。
本地用互斥锁，Redis 用服务端脚本，PG 用每 Resource 的短事务行锁。
低优先级的等待位置和大权重的容量保留必须持久化，不能只存客户端内存。
选择请求时就消耗该优先级槽位并轮转租户；被选中请求随后取消或超时不倒退游标，未被选中请求取消不消耗槽位。

状态主线：`queued -> granted -> released/expired/cancelled`，或 `queued -> cancelled/expired`。
请求只授予一次。queued/granted 记录始终保留，不能因固定幂等窗口结束就删除仍在续租的授权。
到终态后至少保留至 `max(不可变 Submit 截止时间, 终态时间 + 恢复窗口)`；此前禁止清除记录。
Submit 截止时间不可随提交重试或已确认 Renew 刷新；回收后过期 Submit 必须拒绝，旧 ID 禁止复用。
请求引用须携带原始 Submit 截止时间以检查迟到重试；调用方必须遵守 ID 与该截止时间绑定的契约。
细化实现时需给出提交重试上限、终态恢复窗口与垃圾回收策略，不能删记录后把迟到重试当新请求。

## 等待、崩溃与模糊结果

客户端正常等待时定期 Observe；释放、取消、续约和 Observe 都可推进调度。
没有独立 leader 是第一版选择：多个客户端协同触发同一后端原子状态机。
持久排队必须有最大存活期，避免崩溃客户端的队首永久占住轮转位置。
授予时即占用容量并开始 TTL；客户端来不及收到响应，容量也只会保留到 TTL 结束。
如果没有存活客户端推动，不保证主动清扫；新调用必须先清扫，再决定容量。
公平性进展依赖后端可达、持续推进和已有授权最终释放/到期。

Pub/Sub 或 PG 通知只作“尽快 Observe”的提示；即使漏通知仍靠有界间隔轮询推进。
Redis Pub/Sub 是至多一次投递，断开期间消息不会自动重放。[Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/)

网络超时不能解释成回滚，具体处理如下：

| 事件 | 客户端行为 | 后端保证 |
| --- | --- | --- |
| Submit 已提交但响应丢失 | 不执行工作；用同 RequestID 查询/重试 | 不重复入队/扣减 |
| 等待 ctx 取消与 grant 同时发生 | 不交付可用 Lease；用独立短清理 ctx Cancel | Cancel 处理 queued 和 granted；清理失败最多保留至到期 |
| Acquire 成功交付 Lease 后 | 获取 ctx 的结束不自动释放正在运行的工作 | 生命周期由 Lease 的释放/失效管理 |
| Release 结果未知 | 本地永久停止使用 Lease；返回错误，可凭同引用重试/查状态 | 可能已释放，绝不多次归还 |
| Renew 超时/错误 | 本地标为失效、关闭 Done；不得凭乐观猜测继续工作 | 后端可能已续约；等待确认清理或自然到期 |
| 客户端崩溃 | 无需依赖 finally | 排队截止时间与 lease expiry 可回收资源 |

每次 I/O 必须有操作超时。租约本地有效期从请求发出前计时并扣除往返延迟，不能从收到响应起再加完整 TTL。
排队后才授予的场景通过 Observe 返回的后端剩余期限建立保守本地 deadline；响应到达时过期则不交付 Lease。
本地暂停恢复后先检查 deadline；后台定时器没执行不代表仍有效。
续约必须显式或 opt-in；自动续约机制拥有清楚的 Close/停止契约，不执行用户任务。

## Redis driver

同 Resource 的键使用同一 hash tag，避免 Cluster 跨 slot 原子操作。
脚本读取后端时间，回收已到期 leases/queued，修改容量、队列、轮转和状态；所有客户端走同一套脚本。
Redis 脚本执行具有原子可见性，但会阻塞其他服务端操作，必须限定单次回收/授予批量。[Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/)、[Cluster](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)

批量未完成时保存 continuation，下一次操作继续；不能因为达到批量限制绕过被选中的请求。
仅靠键 TTL 不足以维护 InUse：必须原子回收索引与容量计数。活跃资源状态不能被 eviction 任意删除。
脚本错误不具备 SQL 式自动回滚：先验证参数、类型、数值边界，避免写入后才发现可预期错误；发生异常状态拒绝新授权并暴露诊断。

单主实例的原子记账不等于故障切换后强一致。异步复制可丢失已确认授权，副本晋升后重新授予会造成有效授权重叠。
`WAIT` 降低风险，但不能把 Redis 变成强一致 CP 系统。[Redis replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
因此 Redis driver 文档必须给出持久化、复制、故障切换假设；不能承诺跨故障切换零超发。

## PostgreSQL driver

建议 tables：resources（容量/配置/调度游标）、requests（队列及幂等状态）、leases（权重/owner/expiry）。
以 Resource 行 `SELECT ... FOR UPDATE` 串行化该池的短事务：获取锁后读取当前时间，回收、调度、记账并提交。
工作执行期间不持有数据库事务、连接或 advisory lock。不同 Resource 可独立并发。
PG 行锁可阻止其他写入者同时修改该行。[Explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html)

不能用 `SKIP LOCKED` 跳过同一资源的受保护队首来“优化”公平性；它得到不一致视图，适合某些队列消费而非直接替代本提案的次序契约。
可以在不同 Resource 的后台清理任务中评估它。[SELECT](https://www.postgresql.org/docs/current/sql-select.html)
到期判定使用获得锁之后的 `clock_timestamp()`，不使用事务开始时冻结的 `now()`。[Date/time functions](https://www.postgresql.org/docs/current/functions-datetime.html)

COMMIT 回包丢失仍是未知结果，沿用 RequestID/LeaseID 查询，不因数据库支持事务而假设客户端知道结果。
PG 默认流复制也是异步；强持久授权要配置同步复制、可靠存储、适当的 synchronous_commit，并保证故障切换只选择满足条件的副本、隔离旧主。
这些是部署前提，不是 driver 自动提供的功能。[Warm standby](https://www.postgresql.org/docs/current/warm-standby.html)

## 安全边界：租约不是停止工作

`sum(valid lease weights) <= Capacity` 是后端账本保证。进程暂停超过 TTL 后恢复、网络分区或业务忽略 Done 时，
旧工作可能仍在执行，同时容量已分给新工作；因而不是任意故障下的物理运行并发上限。
如果业务要求严格保护资源，需资源端原子验证授权或资源端直接执行受限操作；单独检查一次再执行存在检查后失效的窗口。
外部不可取消的 HTTP 工作等通常无法仅靠 Redis/PG 租约实现这一严格保证。

多个合法 holder 同时存在时，不能用“只接受最大 fencing token”过滤：第二个合法 holder 的 token 会错误废掉第一个。
如确有资源端校验需求，应验证 Resource、LeaseID、Owner、有效状态/epoch，并将授权校验与业务动作原子结合；
跨数据库校验与外部动作不天然原子。第一版不提供通用 fencing 承诺。
时钟跳变、状态丢失和主从切换同样是租约边界；故障后重建空账本不能直接视为安全恢复。

## 分阶段与验收

1. 本地版本先实现权重校验、原子 grant/release 与兼容入口，再以独立变更加入 8:3:1 与租户轮转、防饿死。
2. 提取共用状态机和分布式协议；memory driver 运行协议契约测试，验证 TTL、未知结果和幂等行为。
3. PG driver：先验证事务/持久状态及真实实例故障测试，建立远端协议参考实现。
4. Redis driver：实现等价脚本和 Cluster slot 约束；明确与 PG 不同的持久性及故障切换保证。

两种远端 driver 可以在第二阶段契约稳定后用独立 worktree 并行开发；合入次序建议先 PG，再 Redis。
步骤 1 的本地多令牌与优先级已在当前功能分支实现，见[本地调度契约](002-weighted-scheduling.md)。步骤 2 已在当前 PR 实现；步骤 3 已由 PostgreSQL 独立 PR 实现；步骤 4 已由 Redis 独立 PR 实现，采用 Go 状态机加 Lua 时间窗口 CAS，偏离下文原始的完整 Lua 调度建议。本篇不是远端性能或故障切换验证结论。

| 测试类别 | 验收点 | 环境 |
| --- | --- | --- |
| 原子权重 | 多客户端请求 1/2/Capacity，账本不超发；非法重量不入队 | 全后端同一事件轨迹 |
| 公平性 | 持续高优先级、低优先级仍进展；大请求不被小请求永久插队 | 全后端，确定性屏障 |
| 崩溃 | queued/granted 后杀客户端；截止时间/TTL 后恢复容量 | memory 虚拟时钟 + 真实后端 |
| 竞态 | cancel/grant、release/expiry、renew/expiry，终态不复活 | 并发与 race |
| 模糊结果 | 写成功后丢响应、超时；查询/重试不重复扣减或释放 | 注入传输错误 + 真实实例 |
| 幂等 GC | 迟到重试、参数变更、窗口结束，不重新授予旧请求 | 全后端 |
| 通知/推进 | 丢通知、清理批次耗尽后仍推进；无忙循环 | 全后端 |
| PG | 行锁等待后到期判定、提交丢响应、连接断开、主备切换 | 真实 PG，部署配置记录 |
| Redis | 脚本加载/重试、Cluster 同槽、复制延迟/主故障 | 真实 Redis/Cluster；不以 mock 代替 |
| 依赖边界 | 根包无第三方运行依赖；每模块可独立下载/构建 | go.work 与 GOWORK=off |

实现后对各模块执行 gofmt、go test、go vet，涉及并发执行 race；声明“故障切换通过”必须附故障模型及配置。

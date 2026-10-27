# Driver 契约与 memory 参考实现

更新：PG 的 snapshot 存储方案已由 [009 多表存储决策](009-postgres-relational.md) 取代；
driver 契约不限定持久化布局，结构化状态入口与快照入口都是可选辅助函数。以下保留原决策背景。

状态：core 协议、Shared 客户端、memory 及 PostgreSQL、Redis driver 均已在 `dev` 实现。
本文保留最初 driver 契约的决策背景；部署边界分别见[决策 006](006-postgres-driver.md)与
[决策 007](007-redis-driver.md)；PostgreSQL 的存储布局演进见[决策 009](009-postgres-relational.md)。
不代表已有标签版本。

更新：Shared 支持可选 ResourceManager 的版本化 Resize 与只读 Inspect。Config 中容量是首次创建提示，
当前容量/容量版本独立持久化；不改变请求身份。扩缩容、合法超限、查询时间口径见
[Shared 指南](../guides/shared.zh-CN.md#shared-动态容量)。以下背景中的固定容量不变量应按该契约理解。

## API 与原子边界

公开 Driver 使用五个有明确含义的方法：

```go
type Driver interface {
    Submit(context.Context, Request) (Result, error)
    Observe(context.Context, Request) (Result, error)
    Cancel(context.Context, Request) (Result, error)
    Renew(context.Context, LeaseRef) (Result, error)
    Release(context.Context, LeaseRef) (Result, error)
}

type LeaseRef struct {
    Request Request
    ID      string
}
```

Submit/Observe/Cancel 携带完整、不可变 Request（资源、配置、ID、Owner、租户、重量、提交截止、等待截止）。
Renew/Release 使用 LeaseRef，包含原 Request 和授予结果的 LeaseID；引用本身不证明租约仍有效。
调用者负责可信租户身份和后端访问控制。

取舍：方法数量增加，但调用点直接表达操作，续租和释放明确要求租约引用，不再由调用者拼装 Kind。
返回值保留统一 Result：所有操作都需要表达权威终态及可能发生的取消/过期竞态。
这替换了早期 Execute 契约；自定义 driver 必须实现五个方法，没有旧 Execute 兼容适配器。
高层 Shared 的申请/释放接口不变，本地 Airlock 不受影响。

Command、Operation、ValidateCommand 和 Transition 保留为独立 driver 模块可以复用的状态机工具，
不是 Driver 调用入口。各内置 driver 的五个方法转入同一私有执行路径，不复制调度逻辑，
不改变原子边界、幂等身份、TTL、错误优先级或取消语义。

`Transition(state, backendNow, command)` 返回结构化新状态、Result、CommitWindow 和错误。
Driver 必须串行化整个资源的读、转换、写，并且在新 snapshot 非 nil 时即使业务错误也要提交；
例如队满拒绝需要保存，过期回收可能让其他人获得授权。持久化失败不能返回可用授权。
提交结果可能未知，数据库支持事务也不例外。绝不自动回退到独立的本地容量池。

快照包含有效授权、终态记录、组游标、租户轮转/FIFO、protected head。它复用本地
`enqueue/remove/dispatch`，没有另写一份公平算法；已有本地 Permit API 与取消竞态规则保持不变。
整资源 JSON 是第一版有意选择：易于同契约复用和原子审查，但每次操作 O(保留记录数 + 队列数)，
不是高吞吐承诺。每资源 Capacity/MaxQueued/MaxRecords 最多 10000，不能在终态恢复窗口内为新请求腾空旧记录。
配置只在快照里保存一份。后续按实测拆表/索引，不能改写调度契约来绕开成本。

## 时间、幂等与 GC

持久时间与配置时长统一毫秒；返回 Remaining 扣除后端当前时间的小数毫秒，避免客户端把截断损失误当有效期。所有文本字段要求合法 UTF-8 且无 NUL，防止 JSON 替换字节造成身份漂移，同时与 PostgreSQL text 支持域一致。LeaseTTL、MaxWait、SubmitWindow 各不超过一小时，Retention 不超过一天。
后端在获得独占访问后取时；检测到比上次提交时间回退则拒绝。本地客户端使用单调计时，
按调用开始时间加 Remaining 建立保守 deadline（扣掉整个往返时间）。这不解决后端时钟向前跳变。

ID 编码不可变 SubmitUntil 和 128 位随机值；Owner 为独立随机 128 位值。改截止必须产生新 ID。
相同 ID 不同 Owner/参数是冲突；相同 Submit 不重复排队、扣减或延长有效期。
终态保留至 `max(SubmitUntil, Finished+Retention)`，仍 queued/granted 的记录永不按恢复窗口删除。
过期 ID 被 GC 后仍不能重新提交；历史满返回 ErrHistoryFull，不能驱逐活跃或尚可恢复的记录。
QueueFull/GroupQueueFull/TenantQueueFull 是持久化的 Rejected 结果，重试保持相同错误。

Cancel 可以先于 Submit 到达：创建 Cancelled tombstone，迟到 Submit 只能看到取消终态。
取消与授予竞态都由同一后端原子边界处理；Cancel 也终止已授予授权。成功取消后不会有迟到重生。
如果 Cancel 自身失败（网络/历史容量等），清理不确定，原请求最多存活到等待截止及租约到期。
没有 leader 或主动清扫保证；所有操作都推进过期和调度，无存活调用时不清扫。

## 客户端生命周期

公开命名统一为 `NewShared`、`Shared`、`SharedOptions`，强调通过后端共享并发额度与排队状态；Lease 继续表达授权有效期。
构造时不做后端 I/O、不申请令牌、不接管连接。零值协议参数由构造器补默认值，
完整规则和已有资源迁移边界见 [ADR 008](008-shared-usability.md)。旧 Distributed 系列名称保留为已弃用的兼容入口，
不维护第二份实现；本地 `New(Config)` 不变。

`NewShared(driver, options)` 返回独立的 lease API，提供 Acquire/AcquireFor/AcquireN/AcquireNFor。
每次 I/O 受 OperationTimeout 限制；等待使用 PollInterval 定时 Observe，查询先后不决定授予顺序。
提交或查询遇到错误时不执行工作，以独立短 ctx Cancel，返回原错误及可能的清理错误。
第一版不隐式重试网络错误；需要手动恢复原请求的高级使用者保留 Request 并直接调用 Driver。

等待 ctx 取消时不交付 Lease；已交付 Lease 不继承该 ctx。Lease 有 Valid、Done、Weight、Renew、Release。
续租是显式的，没有隐藏续租 goroutine。响应超时、续租错误或旧本地 deadline 已过，都永久失效；
晚到成功不能复活 Lease。Release 在网络调用之前本地失效，失败允许同对象重试清理。
定时器负责关闭 Done，Valid 还直接检查 deadline，不能用“定时器尚未运行”当成仍然有效。

新增授予必须在当前容量内；平滑缩容允许已有授权总重量暂时超过新 Capacity，此时不新增授予。
这属于授权账本规则，不是任意暂停/分区下的物理运行并发上限。
调用者须在失效时停止工作，且工作真正结束后再释放。库不执行任务、不自动重试业务，也不提供通用 fencing。
多个合法 holder 并存时，单个最大 fencing token 会错误废掉其他合法 holder。

## 模块与测试

core 运行依赖保持标准库；memory 是独立模块。开发不创建 tag，driver 固定到真实已推送的 core commit
对应 pseudo-version；独立 `GOWORK=off` 构建验证不能被本地 replace 替代。正式发布顺序另行授权。

memory 使用同一个 Transition，状态仅存在本进程，同实例共享额度；不同实例彼此独立，重启丢失状态。
终态记录自动按上述规则回收，但资源配置保留到 driver 生命周期结束；不能将它作为持久后端的故障降级。
测试覆盖确定性状态轨迹、完整公平序列/恢复、幂等 GC、时间边界、取消先到、未知结果和 lease 并发竞态。
真实 PG/Redis 的锁等待、连接/提交丢响应、Cluster/failover 边界由各 driver PR 补充，memory 测试不能证明它们。

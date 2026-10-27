# 实现自定义 driver

[English](drivers.md) · [Shared 使用指南](shared.zh-CN.md)

本页面向后端适配器作者。业务调用方通常只使用 Shared 的 Acquire 方法和返回的 Lease，
无需自己构造协议请求。

```go
type Driver interface {
    Submit(context.Context, Request) (Result, error)
    Observe(context.Context, Request) (Result, error)
    Cancel(context.Context, Request) (Result, error)
    Renew(context.Context, LeaseRef) (Result, error)
    Release(context.Context, LeaseRef) (Result, error)
}
```

| 操作 | 契约 |
| --- | --- |
| Submit | 原子授予或排队全部重量；相同请求重试不刷新租约 TTL |
| Observe | 获取结果，必要时推进过期与调度；只读快路径不增加 Revision |
| Cancel | 取消排队或已授予请求；取消允许先于提交到达 |
| Renew | 仅延长仍有效且匹配的租约，不能复活已过期租约 |
| Release | 归还匹配租约的全部重量，最多记账一次 |

Submit 不等待容量，而是返回 Queued、Granted 或错误；调用仍可能等待后端 I/O 或锁，
受 ctx 约束。轮询与取消清理由 Shared 负责，不隐式重试网络错误。

Request 包含资源、不可变策略与首次创建容量提示、ID、Owner、租户、重量、提交和等待截止时间，后续需原样复用。
LeaseRef 包含原 Request，ID 字段是授予时的 LeaseID。Result 表达生命周期状态、租约 ID、
重量、剩余有效期和后端 revision。即使写入可能已提交，带有错误的结果也不能授权工作。

## 复用状态机，保持原子边界

存储布局由 driver 决定：多张 SQL 表、快照等都可以，前提是满足同一原子性契约。参考辅助函数不是强制要求。
三个驱动统一使用结构化入口：

```go
func Transition(state *SharedState, now time.Time, cmd Command) (
    next *SharedState, result Result, window CommitWindow, err error,
)
```

nil 输入表示新资源。调用时转交输入 maps/records 的独占所有权，报错后也不能复用输入。
Transition 不做 I/O；返回非 nil 状态时，即使同时返回业务错误也必须原子持久化。
PostgreSQL 将 SharedState、DriverRecord 映射为关系表；memory 和 Redis 在 Transition
前后使用 DecodeState、EncodeState 处理快照。

Command/Operation/ValidateCommand 是实现工具，不是公开 Driver 的调用接口。
直接调用者必须提供完整 ResourceConfig，只有 NewShared 会补齐默认配置。

提交窗口同时包含转换前后的期限。原地续租也必须保留原租约期限作为提交上界。
使用不会过期的锁、且持有独占访问直到提交的驱动可以忽略窗口。
Redis 的更新锁带 TTL，必须在发布时原子校验 owner 和窗口，不再比较旧快照。
使用乐观更新的自定义驱动则必须比较原状态并校验窗口。
明确的锁竞争允许重试抢锁，不加载或计算完整快照；确认归属失效、比较失败或窗口失效
后才重新读取并计算。传输错误仍表示结果未知。

Redis 的状态操作和锁竞争受 `WithOperationTimeout`（默认五秒）或调用方更早的截止时间
约束。抢到锁后，清理使用独立 context，预算为操作超时与 100ms 的较小值，因此返回
时间可能比截止时间晚最多这段额外预算。校验 owner 的解锁失败后由 TTL 回收，不会撤销
已提交状态，也不会把不确定提交变成成功；最终仍检查操作 context。

参考状态机将边界收集合并到已有记录遍历中，窗口语义保持不变；见
[等价性测试与基准记录](../benchmarks/commit-window/README.md)。

同一资源的读取、转换、提交必须整体串行化。后端拿到独占访问后再取时；使用乐观更新的后端，
必须在原子发布时校验 Transition 返回的时间窗口。返回非 nil 新状态时，
即使同时有业务错误，也要提交拒绝记录、过期回收和调度变化。成功提交后才能返回可用授权，
并遵守 ctx 取消。不能退化为独立本地容量池。

[memory 参考实现](../../driver/memory/memory.go)、
[PostgreSQL 适配器](../../driver/postgres/postgres.go)和
[Redis 适配器](../../driver/redis/redis.go)展示了三种实现方式。
除[公共契约测试](../../internal/driverconformance/conformance.go)外，还需要各后端自己的锁等待、
超时和提交结果未知测试。memory 不能证明持久化、网络故障或故障切换行为。

实现前应阅读完整[契约与取舍](../decisions/005-driver-contract.md)。独立模块依赖和验证方式
见[模块说明](../../MODULES.md)。旧 Execute(ctx, Command) driver 接口已移除，
自定义 driver 需实现上面的五个方法。

只读优化可使用核心 `Observation.Observe` 与 `SharedState.NextMaintenance` 辅助方法。
元数据和目标请求必须来自同一个原子快照，并使用后端时间。到期、结果缺失或索引不完整
时须回到完整 Observe，不能因分离查询而丢失无人 Release 时的过期调度能力。

## 可选资源管理

Driver 可同时实现 ResourceManager，提供 InspectResource 和 Resize。Resize 使用真正的资源级
命令 `Command{Kind: Resize, Resize: ...}`，不伪造用户请求；Command.Resource() 返回目标资源。
发布成功后才返回 Result.ResourceStatus，发布出错则返回零状态。复用正常资源更新边界，
owner/时间窗口重试保持原 ExpectedVersion；容量版本冲突不产生候选状态。

SharedState.Capacity / CapacityVersion 独立于 Config.Admission.Capacity 的首次创建提示。
用 EqualPolicy 比较不可变规则，请求身份比较前归一化提示；普通操作和重启都不能重置容量。
Resize 清理过期时不先按旧容量调度，移除超重等待者后才按新容量调度；恢复必须允许合法超限。
state.Observation() 生成当前容量/版本、占用与排队计数、下一维护时间，并与请求变化一致发布。
Inspect 不运行 Transition。Redis 仍在 Lua 中检查 owner + CommitWindow，不比较旧快照；
PG 在资源行事务中同时写容量、容量版本和请求差异。

ValidateCommand 只校验重量固定范围 1..10000，不校验资源当前容量。自定义 driver 即使
不实现 ResourceManager，也必须在原子边界内按当前容量检查新 Submit；Transition 已包含此规则。

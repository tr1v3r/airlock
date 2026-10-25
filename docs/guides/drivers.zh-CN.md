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
| Observe | 获取结果，同时推进过期与调度，因此可能写状态 |
| Cancel | 取消排队或已授予请求；取消允许先于提交到达 |
| Renew | 仅延长仍有效且匹配的租约，不能复活已过期租约 |
| Release | 归还匹配租约的全部重量，最多记账一次 |

Submit 不等待容量，而是返回 Queued、Granted 或错误；调用仍可能等待后端 I/O 或锁，
受 ctx 约束。轮询与取消清理由 Shared 负责，不隐式重试网络错误。

Request 包含资源、不可变配置、ID、Owner、租户、重量、提交和等待截止时间，后续需原样复用。
LeaseRef 包含原 Request，ID 字段是授予时的 LeaseID。Result 表达生命周期状态、租约 ID、
重量、剩余有效期和后端 revision。即使写入可能已提交，带有错误的结果也不能授权工作。

## 复用状态机，保持原子边界

存储布局由 driver 决定：多张 SQL 表、快照等都可以，前提是满足同一原子性契约。参考辅助函数不是强制要求。
三个驱动统一使用结构化入口：

```go
func Transition(state *DriverState, now time.Time, cmd Command) (
    next *DriverState, result Result, window CommitWindow, err error,
)
```

nil 输入表示新资源。调用时转交输入 maps/records 的独占所有权，报错后也不能复用输入。
Transition 不做 I/O；返回非 nil 状态时，即使同时返回业务错误也必须原子持久化。
PostgreSQL 将 DriverState、DriverRecord 映射为关系表；memory 和 Redis 在 Transition
前后使用 DecodeState、EncodeState 处理快照。

Command/Operation/ValidateCommand 是实现工具，不是公开 Driver 的调用接口。
直接调用者必须提供完整 ResourceConfig，只有 NewShared 会补齐默认配置。

提交窗口同时包含转换前后的期限。原地续租也必须保留原租约期限作为提交上界。
加锁驱动在持有独占访问直到提交的前提下可以忽略窗口。
只有确认 CAS 冲突或窗口失效后才能重新读取并计算；传输错误仍表示结果未知。

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

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

`protocol` 包负责命令、生命周期记录和参考状态机。根 `airlock` 包提供应用 API，
并导出 Driver 契约的类型别名。本地准入与共享协议共同使用 `internal/scheduler`，
复用 FIFO、租户轮转、优先级选择和保护队首规则。

```text
应用 -> airlock.Shared -> Driver 业务方法
                             | 读取 / 加锁或 CAS
                             v
                       protocol.Transition
                             |
                       internal/scheduler
                             ^
应用 -> airlock.Airlock ------|
```

三个驱动统一调用：

```go
func Transition(state *State, now time.Time, cmd Command) (
    next *State, result Result, window CommitWindow, err error,
)
```

实现驱动时导入 `github.com/tr1v3r/airlock/protocol`。`State` 保存 `Record` 和调度布局，
nil 输入表示新资源。调用时转交所有输入 maps/records 的独占所有权；报错后也不能复用。
Transition 不做 I/O。返回非 nil 状态时，即使同时返回业务错误也必须原子持久化；
成功提交后才能返回可用授权。nil 输出不可提交。

| 驱动 | 读取与转换 | 提交 |
| --- | --- | --- |
| memory | 持有资源 gate；DecodeState 后以本地时间调用 Transition | EncodeState 并替换快照，然后释放 gate |
| PostgreSQL | 锁定资源行、读取记录、取得数据库时间，再调用 Transition | 持有事务锁写入变化的行并提交 |
| Redis | 原子读取快照和服务端时间；DecodeState 后调用 Transition | EncodeState；Lua 同时检查原始快照和 CommitWindow 后发布 |

`CommitWindow.NotBefore` 包含边界，`Before` 不包含边界，零值表示没有上界。
窗口同时包含转换前后的租约/排队期限、提交有效期边界和历史回收期限。
续租必须保留原租约到期时间作为提交上界。Redis 只有确认 CAS 冲突或时间窗口失效时
才能重新读取并计算；传输错误表示结果未知，不能当作冲突重试。
加锁驱动在持锁直到提交的前提下可以忽略提交窗口。

`DecodeState` 和 `EncodeState` 只负责编解码。SQL 驱动可以直接将 State、Record 映射为行。
自定义驱动也可以在遵守业务契约的前提下采用自己的状态机和存储格式。
`ValidateCommand` 用于分配资源或开启事务前校验输入。
直接调用协议时需提供完整 ResourceConfig；只有 NewShared 补齐默认值。
驱动必须遵守 ctx 取消，不能退化为独立本地容量池。

[memory 参考实现](../../driver/memory/memory.go)、
[PostgreSQL 适配器](../../driver/postgres/postgres.go)和
[Redis 适配器](../../driver/redis/redis.go)展示了三种实现方式。
除[公共契约测试](../../internal/driverconformance/conformance.go)外，还需要各后端自己的锁等待、
超时和提交结果未知测试。memory 不能证明持久化、网络故障或故障切换行为。

实现前应阅读完整[契约与取舍](../decisions/005-driver-contract.md)。独立模块依赖和验证方式
见[模块说明](../../MODULES.md)。

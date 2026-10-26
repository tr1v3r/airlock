# Memory driver

A test and demo reference for the Shared lease protocol. **For ordinary
single-process concurrency admission, use `airlock.New(Config)` directly.**
This adapter deliberately keeps lease expiry, renewal, and polling semantics. It uses the core
state machine and supports weighted tenant/group scheduling, TTL, cancellation,
idempotent recovery, and explicit renewal. It owns no workers or connections.

Start with the runnable examples (from this module directory):

```sh
go run ./examples/hello       # Acquire and release without external services.
go run ./examples/lifecycle   # Explicit renewal, task cancellation, joined cleanup.
```

Follow the [Shared user guide](../../docs/guides/shared.md) for defaults and lifecycle
management. This module is not a persistent backend or a substitute for local Permit.

Create `memory.New()` and pass it to `airlock.NewShared`. Share **one Driver
instance** between clients that must share a capacity pool. See the runnable
[example](example_test.go) and [contract](../../docs/decisions/005-driver-contract.md).

State is volatile. Restart or another driver instance starts a different pool;
never use it as an automatic fallback for PostgreSQL/Redis. Terminal records are
reclaimed on subsequent commands, but resource configurations remain allocated
until the driver is discarded. Lease expiry cannot stop business work.

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

The public operations are `Submit`, `Observe`, `Cancel`, `Renew`, and `Release`.
The first three accept an immutable `protocol.Request`; renewal and release use
`protocol.LeaseRef{Request: original, ID: result.LeaseID}`. All return `(protocol.Result, error)`.

## 中文

memory 主要用于 Shared 租约协议的测试和演示。**普通单进程并发控制直接用
`airlock.New(Config)`。** 此 adapter 有意保留租约过期、续租与轮询语义。
在本模块目录运行上面的 hello 和 lifecycle 示例，或阅读
[Shared 使用指南](../../docs/guides/shared.zh-CN.md)了解默认配置和任务生命周期。
它不是持久化后端，也不替代本地 Permit。

实现复用 core 状态机，支持多令牌、租户/优先级公平队列、
租约过期、幂等恢复、取消和显式续租。共享容量的客户端必须共享同一个 `memory.New()` 实例。
参见上面的可运行示例。它不持久化、不启动业务 worker，重启或新实例会创建独立容量池，
不能作为远端故障时的自动降级。终态记录在后续操作中回收，资源配置保留到 driver 丢弃为止。
租约过期仅回收账本额度，业务必须自行停止工作。

公开操作为 `Submit`、`Observe`、`Cancel`、`Renew` 和 `Release`。前三者接收不可变
`protocol.Request`；续租与释放接收 `protocol.LeaseRef{Request: original, ID: result.LeaseID}`，
均返回 `(protocol.Result, error)`。

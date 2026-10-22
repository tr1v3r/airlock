# Memory driver

An in-process reference for the Airlock distributed protocol. It uses the core
state machine and supports weighted tenant/group scheduling, TTL, cancellation,
idempotent recovery, and explicit renewal. It owns no workers or connections.

Create `memory.New()` and pass it to `airlock.NewDistributed`. Share **one Driver
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

## 中文

memory 是分布式协议的进程内参考实现，复用 core 状态机，支持多令牌、租户/优先级公平队列、
租约过期、幂等恢复、取消和显式续租。共享容量的客户端必须共享同一个 `memory.New()` 实例。
参见上面的可运行示例。它不持久化、不启动业务 worker，重启或新实例会创建独立容量池，
不能作为远端故障时的自动降级。终态记录在后续操作中回收，资源配置保留到 driver 丢弃为止。
租约过期仅回收账本额度，业务必须自行停止工作。

# 决策 002：按次选择立即获取

状态：本分支实现的本地接口，尚不代表已合入或发布。适用于 `TryAcquire`、`TryAcquireFor`、`TryAcquireN` 和 `TryAcquireNFor`。本地优先级扩展见[优先级决定](003-local-priority.md)；分布式 driver 仍未实现。

## 问题与选择

同一个容量池可能服务两类调用：后台任务可以有界排队，在线请求需要容量不足时立即降级。仅通过创建时的队列上限无法按次选择；分别创建 Airlock 又不能共享同一个总容量。

保留现有 `Acquire` 的等待契约，新增四个与之对称的入口：

```go
TryAcquire() (*Permit, error)
TryAcquireFor(tenantID string) (*Permit, error)
TryAcquireN(n int) (*Permit, error)
TryAcquireNFor(tenantID string, n int) (*Permit, error)
```

默认租户和显式租户仍然独立；单令牌入口等价于权重 1。成功返回原有 `*Permit`，调用方通过 `Release()` 全量、幂等地归还。没有部分获取或预留。

## 容量与公平性契约

命名租户先验证 ID，随后验证权重；空 ID 返回 `ErrInvalidTenant`，`n <= 0` 返回 `ErrInvalidWeight`，`n > Capacity` 返回 `ErrWeightExceedsCapacity`。

合法请求在同一把互斥锁内检查：只有没有等待请求，且 `n <= Capacity - InUse` 时才能授予全部权重；否则返回 `ErrWouldBlock` 和 nil Permit。失败不改变容量、队列或租户轮转，也不会消耗排队额度。全局和单租户队列上限均不影响 Try 的判断。

例如容量为 5，已有 Permit 持有 3，A 的 4 令牌申请在等待。即使还有 2 个空闲令牌，B 的 `TryAcquireN(1)` 仍返回 `ErrWouldBlock`。允许 B 获取会绕过已经排队的大请求，重新引入无限插队风险。因此保留[本地多令牌取舍](001-local-weighted-tradeoffs.md)中的大请求保护，并接受它造成的闲置容量。

Try 不加入轮转，所以没有“下次一定成功”的保证，也不获得等待者的进展保证。想等待时应该调用带 context 的 Acquire，并配置正数队列上限；不要循环 Try 来模拟排队或实现公平性。循环重试既增加锁竞争，也没有排队位置。

## “立即”的边界

本地 Try 不接收 context，不等待容量、通知 channel 或队列位置，但仍需获取内部互斥锁。其他线程可能正在释放和调度，操作系统也可能暂停当前 goroutine，因此它不是 wait-free，不保证确定的返回时长。

成功后的工作仍由调用方执行和取消；Try 不执行任务，也不会自动回收已授予的 Permit。

## 为什么不使用已有手段

| 方案 | 局限或取舍 |
| --- | --- |
| 将队列上限设为 0 | 对整个实例生效，无法让同一实例的一部分调用排队、另一部分立即降级；仍可用于所有调用均拒绝排队的场景 |
| 用已到期 context 调用 Acquire | Acquire 在容量检查前返回 context 错误，即使有容量也不会成功，不等价于 Try |
| 用非常短的 deadline | 依赖时间和调度，仍可能入队，也不能明确表达绝不等待容量 |
| 返回 `(*Permit, bool)` | 无法同时区分暂时不可用、非法租户、非法权重和超过总容量；使用 error 与 Acquire 保持一致 |
| 容量足够就绕过队列 | 能提高部分场景利用率，但破坏已有请求的顺序和大请求保护；当前不采用 |

`ErrWouldBlock` 是可用性结果，与 `ErrQueueFull` / `ErrTenantQueueFull` 不同：Try 根本没有尝试入队。调用方应通过 `errors.Is` 将它与输入错误区分，只对暂时无法准入执行降级，而不是掩盖配置或参数问题。

## 验证边界与重新评估条件

需要验证四个入口的权重和租户语义、输入错误优先级、零队列额度下的成功、容量不足拒绝、已有等待者时拒绝、失败前后统计和轮转不变，以及并发 Try/Acquire/Release 的容量守恒。可运行示例见 [tryacquire](../../examples/tryacquire/example.go)。

如果业务以后要求允许部分请求插队，需要重新定义调度策略及防饥饿保证，不能悄悄改变 Try 的公平性条件。分布式 driver 涉及网络超时和结果不确定性，也需要单独设计，不能由当前无 context 的本地接口直接推导其契约。

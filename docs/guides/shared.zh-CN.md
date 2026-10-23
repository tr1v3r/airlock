# Shared 入门：从配置到任务清理

[English](shared.md) · [返回 README](../../README.zh-CN.md)

普通单进程并发控制用 `airlock.New`；多个进程通过 Redis 或 PostgreSQL 共享容量时，
用 `airlock.NewShared`。memory adapter 是测试和演示租约 API 的参考实现，
不是普通本地准入入口，也不是持久化后端。

## 1. 选择后端，配置容量与排队

先按 [Redis 接入](../../driver/redis/README.zh-CN.md)或
[PostgreSQL 接入](../../driver/postgres/README.md#中文)创建 driver。
连接客户端或连接池由应用持有；PostgreSQL 建表是显式的部署步骤。
有了 `driver`，构造只需要：

```go
shared, err := airlock.NewShared(driver, airlock.SharedOptions{
    Resource: "jobs",
    Config: airlock.ResourceConfig{
        Admission: airlock.Config{
            Capacity: 10, MaxQueued: 20, MaxQueuedPerTenant: 10,
        },
    },
})
if err != nil {
    return err
}
```

必须提供资源名和正数容量。两个队列上限可选，零表示不等待，与本地 `New` 相同；
`MaxWait` 有默认值并不代表开启排队。只用默认租户时，可以把两个队列上限设成相同值，
让它使用全部等待额度。构造时不访问后端、不启动 worker。

## 2. 用相同的方法名申请

| 需求 | 本地与 Shared 的调用 |
| --- | --- |
| 默认租户、单令牌 | `Acquire(ctx)` |
| 指定租户、单令牌 | `AcquireFor(ctx, tenant)` |
| 默认租户、多令牌 | `AcquireN(ctx, n)` |
| 指定租户、多令牌 | `AcquireNFor(ctx, tenant, n)` |

Shared 返回 `*Lease`，本地返回 `*Permit`。开启等待后，申请会等待到授予、拒绝、取消或
等待截止，无需业务自行写轮询。`TryAcquire` 目前仅本地 `Airlock` 提供。

在包含本 API 的仓库 checkout 中，可以运行不依赖外部服务的完整 hello 示例：

```sh
cd driver/memory
go run ./examples/hello
```

这里使用 memory 只是为了演示 `NewShared`；不同 memory driver 实例不共享容量，
重启会丢失状态。已发布版本可能尚未包含此开发阶段 API。

## 3. 管理任务和租约的生命周期

Permit 持有到主动释放，Lease 还有有效期。传给 `Acquire` 的 ctx 只约束申请过程，
取消它不会撤销已经返回的租约。若希望同一个请求 ctx 也停止业务，需显式传入任务管理代码。

[可运行的生命周期示例](../../driver/memory/examples/lifecycle/main.go)包含应用侧管理代码
和故障时序测试：

```sh
cd driver/memory
go run ./examples/lifecycle
go test -race ./examples/lifecycle
```

它完整展示以下顺序：

1. 申请成功后才开始业务。
2. 给业务传入可取消的 ctx，独立监听 `lease.Done()`，避免续租 I/O 阻塞时延迟发送过期取消信号。
3. 业务运行期间显式续租。任何续租错误都停止业务，不自动重试业务；续租间隔和 I/O 超时
   应在 TTL 内留出充分余量。
4. 等业务及其启动的所有子任务实际结束，再停止并等待续租和监听 goroutine 退出。
5. 使用独立于已取消工作 ctx 的短超时 ctx 释放。释放失败时结果可能未知；示例仅对
   **同一个 Lease** 重试一次清理，不重跑任务。两次都失败时，把清理错误与业务错误一起返回。

为快速看到续租，示例把 TTL 缩短到 1 秒，每 250ms 续租，操作 ctx 为 100ms。
这些是 memory 演示参数，不是远端后端的调优建议。使用默认 30 秒 TTL 时，应用可以从
每 10 秒续租、5 秒 I/O 上限开始，根据实际延迟调整；任何参数都无法保证承受任意长的
进程暂停或网络分区。

SDK 不执行任务、不自动续租，也无法杀死忽略取消信号的工作。示例中的 `runJob` 是应用代码，
不是新增 SDK API。租约到期回收的是账本额度，不能阻止旧任务继续写外部系统；此类写入
需要业务针对下游增加保护。

## 4. 按需调整高级参数

对于下表的协议和轮询设置，仅在 `NewShared` 中，**零值**选择默认值；显式非零值保持原样，
非法值仍报错。`ClockSkewAllowance` 则显式启用：零表示不预留时钟偏差预算。
直接调用 Driver 时不自动补默认值。已弃用的 `NewDistributed` 转发到同一实现，默认规则相同。

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `Config.LeaseTTL` | 30s | 授予或成功续租后的有效期 |
| `Config.MaxWait` | 30s | 最大排队时间；调用者更早的 deadline 优先 |
| `Config.SubmitWindow` | 5s | 初次请求允许提交的时间窗口 |
| `Config.Retention` | 1m | 请求进入终态后的恢复记录保留时间 |
| `Config.MaxRecords` | 10000 | 活跃、排队和保留终态记录的总上限 |
| `OperationTimeout` | 5s | 客户端每次后端调用的时间上限 |
| `PollInterval` | 50ms | 排队期间两次查询之间的间隔 |
| `ClockSkewAllowance` | 0 | 显式预留已知客户端/后端时钟偏差上界 |

终态记录需要同时满足提交窗口和保留时间条件后才能删除。历史满时，即使容量空闲，也可能
返回 `ErrHistoryFull`。MaxRecords 是按需增长的记录上限，不是预分配容量；历史越多，
快照计算成本越高。这些默认值是起步策略，不保证吞吐或延迟。driver 自身更短的操作上限，
以及调用者更早的 deadline，仍可优先生效。

同一资源的所有参与者必须使用相同的**补齐默认值后的 ResourceConfig**。默认值在发往后端前
补齐。已有资源的配置不会被改写：升级时若省略了以前显式配置且与默认值不同的参数，会得到
`ErrConfigConflict`，不会在线迁移策略。升级时保留现有显式策略，或规划切换到另一个资源。
OperationTimeout、PollInterval 与 ClockSkewAllowance 是客户端自己的设置。

### 为远端客户端显式预留时钟偏差预算

不配置预算时，若客户端时钟领先后端的量超过命令传输耗时，其绝对提交/等待截止就会超过
后端允许的窗口，导致反复返回 `ErrInvalidRequest`。单纯增大窗口不能消除偏差。
应根据**部署环境中已知的时钟偏差上界**选择预算，不存在通用推荐数值：

```go
options.ClockSkewAllowance = 200 * time.Millisecond // 仅当它覆盖本部署的时钟偏差。
```

记预算为 `A`。零值精确保留原截止构造行为。正值必须是整毫秒，并且在补齐默认值后同时满足
`2*A < Config.SubmitWindow`、`2*A < Config.MaxWait`，否则构造返回 `ErrInvalidConfig`。
客户端按以下方式构造截止：

```text
SubmitUntil = floor_ms(clientNow + SubmitWindow - A)
WaitUntil   = floor_ms(min(clientNow + MaxWait, callerDeadline) - A)
```

没有调用者 deadline 时省略该项。即使调用者 deadline 更早，也会扣除预算，不会延长调用者
允许的等待时间。若正预算消耗了整个本地等待余量，`Acquire` 在不访问后端的情况下返回
`ErrLeaseLost`；这不表示 ctx 自身已经过期，已经取消的 ctx 仍返回其自身错误。
偏差在 `±A` 内时，后端的实际窗口最短可为 `window - 2*A`，还需扣除传输/处理时间；
较短的调用者 deadline 即使通过本地预检，也可能已在后端过期。

该设置不是时钟同步或偏差检测。预算不足时，后端仍可能拒绝请求或判定过期；错误不会授权
执行工作，也不会自动换新身份重试。请求 ID 仍绑定调整后的提交截止，后端校验、取消墓碑和
GC 规则均不变。租约 Remaining/RTT 计算不变：预算不会增加租约时长，也不能处理任意时钟
跳变、网络延迟或提供 fencing。设计取舍见 [ADR 009](../decisions/009-clock-skew.md)。

协议实现细节放在独立的[自定义 driver 指南](drivers.zh-CN.md)。默认值取舍见
[ADR 008](../decisions/008-shared-usability.md)。

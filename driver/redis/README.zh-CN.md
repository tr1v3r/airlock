# Airlock Redis driver

[English](README.md)

独立模块 `github.com/tr1v3r/airlock/driver/redis`。根模块运行时仍只依赖标准库；
此 adapter 使用 `go-redis/v9`，miniredis 仅供测试。Redis 需支持 Lua 和 `TIME`。

接入业务先看 [Shared 使用指南](../../docs/guides/shared.zh-CN.md)：只配置资源、容量和队列
即可开始，高级协议参数有默认值。该指南提供显式续租、任务取消和释放清理的完整示例。
下面的存储实现细节供部署和后端维护时参考。


```go
client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379", ContextTimeoutEnabled: true,
})
defer client.Close()
namespace := "my-service"
driver, err := redisdriver.New(client, namespace)
// 处理 err，然后将 driver 传入 airlock.NewShared。
```

[可运行示例](examples/basic/main.go)：

```sh
AIRLOCK_REDIS_ADDR=127.0.0.1:6379 go run ./examples/basic
```

`New` 不访问网络，接受 `*redis.Client` 或 `*redis.ClusterClient`，不关闭或修改
调用方的连接。必须启用 `ContextTimeoutEnabled`；拒绝 Cluster 只读/副本路由。
必须保留 socket deadline 设置：go-redis 规范化后会禁用 deadline 的配置会被拒绝，
包括原始 `ReadTimeout`/`WriteTimeout=-2` 及其他此类负数。原始 `-1` 可接受：
它只取消配置的 socket 超时，context 截止时间仍有效。
`WithOperationTimeout` 限制操作（默认五秒），包含更新锁竞争；调用方更早的截止时间优先。
取消或超时后的尽力清理最多额外耗时 100ms。`WithUpdateLockTTL` 配置更新锁租期
（默认一秒），正值向上取整到毫秒；它不改变业务租约的 TTL。
不得修改 client options，或安装会重试结果不确定操作的 hooks。

## 原子性、时间和重试

每个资源使用一个永久 key：`airlock:{sha256(namespace + NUL + resource)}:state`。
namespace 必须为合法 UTF-8、无 NUL、最多 256 字节；hash tag 保证每个操作只访问
一个 Cluster slot。key 为 hash：`state` 保存权威 JSON 快照，`meta` 保存观察元数据，
`r:<request-id>` 保存单个请求索引。区分 key 缺失与 hash 缺少快照字段；
空、损坏或错误类型的状态均拒绝操作。key 不过期，空闲时也保留配置和调度游标。共享资源的所有参与者须使用相同
且不可变的资源配置。

修改状态或到期维护时，先竞争 `airlock:{同一资源哈希}:update-lock`。这是同一
Cluster slot 内独立的 string key；用新的随机 owner token 执行 `SET NX PX` 抢占。
抢锁失败只退避，不加载或计算完整快照。`Submit`、`Cancel`、`Renew`、`Release`
都走这条更新路径。

抢到锁后，Lua 原子读取快照和 Redis `TIME`，Go 计算状态转换。提交脚本在同一次
原子操作中同时校验**当前锁归属和 CommitWindow**，窗口覆盖租约到期、
等待/提交截止和历史回收。后端时间保留微秒精度，限定在 Lua 可精确表示的整数范围。
所有状态写入者都必须先获取同一资源的更新锁。提交只传 owner、新状态、窗口边界和
观察索引，不传回或比较旧快照。持锁期间时间仍会流逝，因此必须保留时间窗口检查；
进程暂停导致锁过期后，即使快照未变，旧持有者也无法提交。

每次成功抢锁的尝试结束后，单独的 Lua 脚本校验 owner 并删除自己的锁，取消或出错
也会清理。清理使用独立且有界的 context，失败则依靠 TTL 回收，不改变命令结果。
抢锁结果不确定时直接返回错误，不重试、不计算状态；可能已经安装的锁同样由 TTL
回收。不创建后台续锁 goroutine；更新锁租期应足以覆盖有界快照的计算和提交。明确
失去锁时丢弃候选、清理残余归属，在操作截止前重新读取计算。锁只覆盖 driver 状态
更新，不跨越业务任务执行。

脚本先准备带独立候选标记的请求索引，最后一条 `HSET` 同时发布权威快照与
匹配的观察元数据，提交脚本内之后不再执行命令；解锁使用独立脚本。索引缺失或标记
不匹配时回退到权威快照，不会授权执行。标记独立于 Revision，防止相同版本的失败候选被误认成成功提交。
Redis 脚本不回滚之前的写入；准备阶段失败只允许后续回退，不允许使用候选授予。

只有明确的锁竞争、归属失效或时间窗口失效才进行 1–10ms 有界退避，
受 context 约束。每条脚本命令通过 `Cmder.NoRetry` 禁止 go-redis 网络重试；明确的
`NOSCRIPT` 才回退到 `EVAL`。Cluster 明确的 `MOVED`/`ASK` 仍可路由。传输或脚本
错误返回零 Result，因为写入可能已经成功；尤其不会在 adapter 内重试不确定的
续期。业务拒绝仍提交过期清理和调度变化。业务执行期间不持有连接。

普通 `Observe` 仅原子读取观察元数据、目标请求和后端时间。在下次租约/等待到期
或历史回收边界之前，直接返回已提交结果，不读取完整快照、不恢复调度器、
不获取更新锁、不改变 Revision、不发布状态。索引缺失或维护到期时才尝试抢更新锁；
抢到后再次检查只读投影，若其他实例已完成维护，释放锁并直接返回，不读取完整状态。
竞争者每次重试前也重新检查投影，可以在不抢到锁的情况下读取新的已提交结果。

修改状态仍需读写有界快照并维护请求索引，成本为 O(records)，且索引增加内存占用。
没有新增 Pub/Sub 或后台 worker；每个等待申请仍默认每 50ms 轮询一次，本次降低
单次轮询成本，不减少轮询调用者数量。不同 Resource 使用独立状态 key 和更新锁，但仍共享
后端 CPU、网络和连接池。入队顺序按成功提交的状态转换确定；更新锁本身不承诺
FIFO，调用先后和轮询频率都不构成授予顺序保证。

## 运行边界

使用独立、受访问控制、**禁止 eviction** 的 Redis；活跃期间不允许外部修改、
设置 TTL、删除或恢复状态。容量安全依赖权威状态不丢失。Redis 异步复制及故障
切换可能丢失已确认状态；持久化和副本本身不会让此 adapter 成为线性一致的分布式
锁。它不提供外部 fencing token。旧持有者仍执行时丢失 key，可能超发容量。
需要此类故障保证的业务应选择适当配置的事务后端，并在需要时对外部资源做 fencing。

调用方必须在 `Lease.Done` 关闭或 `Valid` 为 false 时停止实际工作。租约过期
仅回收记账容量，不能停止 CPU 或撤销写入。显式续期，先停止工作再释放，业务
重试由调用方处理。申请 context 只约束等待，不控制已交付租约。不确定的续期
会使本地租约失效；清理可使用同一 lease 重试，不能以新身份替换不确定请求来
假定已有容量。

资源 key 永久保留。管理删除或容量变更前须停止所有客户端及活跃工作。
Retention/MaxRecords 限制单资源历史，不限制资源 key 总数。权重、优先级选择、
受保护队首和终态幂等均遵循核心 driver 契约。优先级轮次按请求数计算，不按资源
时间计算，也不保证延迟 SLA。高优先级资源可使用独立 key 和固定容量预算隔离。

## 验证

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
# 使用独立测试实例：集成测试会清空脚本缓存。
AIRLOCK_REDIS_ADDR=127.0.0.1:6379 GOWORK=off go test -race -run TestIntegrationStandalone ./...
AIRLOCK_REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 GOWORK=off go test -race -run TestIntegrationCluster ./...
```

容器 Cluster 宣布的地址不能从宿主机访问时，可设置测试专用映射：
`AIRLOCK_REDIS_CLUSTER_MAPPING=containerIP:6379=127.0.0.1:port,...`。
测试覆盖公共契约、更新竞争、过期保护、更新锁竞争/过期/归属校验、持更新锁时的只读
轮询、脚本重载、丢写响应和客户端租约生命周期。
默认测试使用 miniredis；真实 standalone 和 Cluster 集成测试通过环境变量开启。

公开操作为 `Submit`、`Observe`、`Cancel`、`Renew` 和 `Release`。前三者接收不可变
`airlock.Request`；续租与释放接收 `airlock.LeaseRef{Request: original, ID: result.LeaseID}`，
均返回 `(airlock.Result, error)`。

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
`WithOperationTimeout` 限制整个操作（默认五秒）；调用方更早的截止时间优先。
不得修改 client options，或安装会重试结果不确定操作的 hooks。

## 原子性、时间和重试

每个资源使用一个永久 key：`airlock:{sha256(namespace + NUL + resource)}:state`。
namespace 必须为合法 UTF-8、无 NUL、最多 256 字节；hash tag 保证每个操作只访问
一个 Cluster slot。区分 key 缺失和已有空字符串：空、损坏或错误类型的状态均拒绝
操作。key 不过期，空闲时也保留配置和调度游标。共享资源的所有参与者须使用相同
且不可变的资源配置。

Lua 原子读取状态和 Redis `TIME`；Go 计算共享状态机；第二段 Lua 比较原始值并
校验提交时间窗口，最后只执行一次 `SET`。窗口覆盖活跃租约到期、等待/提交截止
和历史清理边界，防止旧快照续期复活已到期租约。后端时间保留微秒精度，并检查
Lua 可精确表示的整数范围。全部校验在 `SET` 前完成，之后没有可能失败的命令；
Redis 脚本不会回滚先前写入。

只有明确的比较失败或时间窗口失效才进行有界退避，并重新读取计算，受 context
约束。每条脚本命令通过 `Cmder.NoRetry` 禁止 go-redis 网络重试；明确的
`NOSCRIPT` 才回退到 `EVAL`。Cluster 明确的 `MOVED`/`ASK` 仍可路由。传输或脚本
错误返回零 Result，因为写入可能已经成功；尤其不会在 adapter 内重试不确定的
续期。业务拒绝仍提交过期清理和调度变化。业务执行期间不持有连接。

每次操作读取、编码并写回整个有界快照，使 memory/PostgreSQL/Redis 共享一致的
调度契约，代价是 O(records) 数据传输和计算，以及热点资源上的 CAS 重试。
资源之间没有全局锁；没有 Pub/Sub 通知、后台清理和自动续期。核心分布式客户端
采用有界轮询；请求在后端的到达顺序决定入队顺序，轮询频率不决定获准顺序。

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
测试覆盖公共契约、CAS 竞争、过期保护、脚本重载、丢写响应和客户端租约生命周期。
默认测试使用 miniredis；真实 standalone 和 Cluster 集成测试通过环境变量开启。

公开操作为 `Submit`、`Observe`、`Cancel`、`Renew` 和 `Release`。前三者接收不可变
`protocol.Request`；续租与释放接收 `protocol.LeaseRef{Request: original, ID: result.LeaseID}`，
均返回 `(protocol.Result, error)`。

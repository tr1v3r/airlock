# Redis driver 的重要取舍

状态：独立 Redis 模块复用核心状态机，使用 Resource 更新锁与 owner/CommitWindow 校验。
实现说明更新日期：2026-10-27。没有创建 tag/release。

## 1. 复用 Go 状态机，以 Lua 发布快照

公平调度使用同一份 Go `Transition`：队列、组游标、租户轮转、protected head 和
完整权重的授予都在核心计算。状态存储在单个 Hash，`state` 是权威 JSON 快照，
`meta` 和 `r:<id>` 是带发布 token 的只读投影。

普通 Observe 原子读取元数据、目标请求和 Redis TIME，在无须维护时直接返回。
维护到期或投影不完整时才竞争更新锁；抢到后再次检查投影，避免其他实例已完成维护后
重复转换。其他写命令直接竞争更新锁。竞争失败不读取或计算完整快照。

持锁者读取完整快照和后端时间，在 Go 中计算新状态，再通过 Lua 发布。单次更新仍为
O(records)；锁减少同资源竞争时的重复计算，没有实现按需加载或增量转换，也不代表
已经实测优于其他存储方案。MaxRecords 限制保留记录数，但应按业务测量快照大小和争用。

## 2. 原子校验 owner 和 CommitWindow

每次尝试获取更新锁都生成新的随机 owner，用 SET NX PX 安装有界租约。默认 TTL 一秒，
可通过 WithUpdateLockTTL 配置，不创建后台续锁 goroutine。提交脚本原子检查当前 owner，
锁过期后，即使状态没有被其他客户端修改，旧持有者也不能提交。

所有状态写入者都必须遵守同一资源更新锁。提交只传 owner、新状态、窗口上下界与观察索引，
不传旧快照或存在性预期，也不执行快照 CAS 比较。

持锁不阻止时间流逝。读取与发布之间仍可能跨过租约、等待、提交或历史回收期限，
因此提交脚本必须再次检查核心返回的 `[NotBefore, Before)`。下界拒绝相对于读取时刻的
时钟倒退，上界包含转换前后的生命周期边界；续租也必须保留原租约期限作为上界。
归属或窗口失效时，候选不会发布，必须重新抢锁、读取、计算。

脚本先验证参数、owner、时间和观察索引，再准备带候选 token 的请求记录，最后一条
HSET 同时发布权威状态和匹配的元数据。Lua 错误不回滚已经发生的写入，投影缺失或
标记不匹配必须回退权威状态，不能授权执行。永久状态 Hash 缺少 state、空或错误类型
均视为损坏，不能重新初始化覆盖。

每次持锁尝试结束后，独立脚本校验 owner 后删除自己的锁；旧 owner 不能删除后继的锁。
取消后的清理使用独立且有界的 context，最多额外耗时 100ms；失败由 TTL 回收。
锁只覆盖状态更新，不跨越业务任务执行。

租约剩余时间按读取时的 backend time 计算；客户端扣除完整调用耗时，不把锁等待或
提交等待额外加到租约上。Redis 服务器时钟仍是部署假设。

## 3. 只重试明确未提交的尝试

锁竞争、owner/window miss 明确没有发布状态，可在操作截止前以 1–10ms 有界退避重试。
Observe 每次重试前重新检查只读结果，可能在不抢到锁的情况下看到其他实例的维护结果。
NOSCRIPT 明确没有执行，允许 EVALSHA 回退到 EVAL；其他抢锁或提交错误返回零 Result，
不自动重放。结果不确定的抢锁可能留下租约，由 TTL 回收；不确定的提交不能授权工作。

每条命令通过 NoRetry 禁止不确定网络错误后的内部重试，保留明确的 Cluster MOVED/ASK
路由。构造器不修改调用方客户端配置，要求 ContextTimeoutEnabled 和 socket deadline，
拒绝副本读取。默认操作上限五秒，调用方更早的期限优先。业务拒绝仍可能伴随非 nil
维护状态提交，不能因为返回队满等错误就丢弃状态变化。

## 4. 一个资源一个 Cluster slot

状态 key 为 `airlock:{sha256(namespace + NUL + resource)}:state`，更新锁为同一 hash tag
下的 `:update-lock`。不同 namespace 或 Resource 使用独立的状态与锁；同一资源的多个
Driver 实例竞争同一把锁。更新锁不承诺 FIFO，入队顺序按成功提交的状态转换确定。
不同资源仍共享后端 CPU、网络和连接池，单个热点资源不能靠 Cluster 自动拆分。

## 5. 永久状态与部署约束

权威状态 key 不设置 TTL，更新锁设置 TTL。使用 noeviction，活跃期间禁止外部删除、
过期、恢复或修改状态与更新锁。终态记录可按协议回收，整个资源的退休需要停止客户端和
活跃业务后处理；状态 key 不自动 GC。客户端与连接资源由调用方管理。

异步复制与故障切换可能丢失已确认状态，更新锁不改变这个边界。租约只能回收记账容量，
不能停止业务任务，也不提供外部 fencing。应用仍须遵守 Lease.Done/Valid 并监督实际工作。

## 6. 验证证明到哪里

测试覆盖公共契约、owner/CommitWindow 拒绝、锁过期、跨客户端竞争、取消后清理、
响应丢失、只读 Observe、维护去重及投影发布。真实 standalone 和三主 Cluster 运行
共同契约、同 slot 多 key 提交、独立客户端、原生 TTL 与 SCRIPT FLUSH 路径。
这些是正确性验证，不是吞吐、P99 或 Cluster failover 安全保证。

`scripts/redis-failover.sh` 构造异步复制缺失，验证旧授权仍有效时，丢失状态的新主可能
再次授予满容量。这用于暴露部署局限，不是宣称故障切换安全。脚本只操作自行创建的隔离资源。

参考：[Redis 脚本语义](https://redis.io/docs/latest/develop/programmability/eval-intro/)、
[复制和故障边界](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)。

## 初版验证记录

实际验证记录（2026-10-23）：Go 1.24.13 / Darwin arm64；根模块覆盖率 100%，
Redis 生产文件覆盖率 97.22%，独立模块的 race/vet/lint 通过。真实 Redis 7.4 standalone、
Redis 7 三主 Cluster 契约测试，以及异步复制缺失 seed/verify 实验通过。
CI 增加 Redis 7/8 standalone、Cluster 和异步故障边界检查；这些是正确性验证，未给出吞吐或 P99 承诺。

审查后补充：构造校验拒绝禁用 socket deadline 的选项；默认和仅 context 超时配置均通过
net.Pipe 黑洞读写测试。Redis 模块最终在 race/覆盖率插桩下重复 5 轮通过；并发契约仍要求
24 个请求全部完成且容量从不超额，轮询间隔使用 50ms，不把此检查当作性能 SLA。

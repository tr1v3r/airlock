# Redis driver 的重要取舍

状态：本 PR 实现独立 Redis 模块；依赖 core 协议、memory 和 PostgreSQL PR。
没有创建 tag/release。核验日期：2026-10-23。

## 1. 复用 Go 状态机，以 Lua 发布快照

第一版没有把公平调度再翻译成 Lua。读取脚本原子返回 GET 与 TIME，Go 运行
`protocol.ApplyOptimistic`，发布脚本检查原值、存在性和 CommitWindow 后 SET。队列、组游标、
租户轮转、protected head 和完整权重的授予都来自同一份 core 状态机。
Lua 不遍历请求队列，也没有服务端维护循环；成本仍与快照字节数有关。

代价是每次通常两次网络往返（脚本缓存未命中时更多）、完整快照传输和客户端重算。
同资源竞争的失败 CAS 会浪费计算与网络；不同资源独立。它不是已经实测优于行锁或 Lua
调度的性能结论。MaxRecords 限制保留记录数；记录和租户名也有界，但上限组合仍可能很大，
应按业务缩小配置并测量快照大小和争用。资源 key 不自动 GC。

## 2. CAS 比较内容还不够，必须检查时间

即使快照没有被其他客户端修改，读与写之间也可能跨过租约、等待、提交或历史回收期限。
core 返回 `[NotBefore, Before)`：下界拒绝相对于读取时刻的时钟倒退，上界包含旧/新状态的
生命周期边界、当前命令期限以及将来才进入有效范围的时间边界。发布脚本重新 TIME，
越界便丢弃候选结果，从新快照重算。失败的候选状态不会消耗调度轮次。

空 key 与不存在分别比较，已有空值视为损坏；不能把损坏状态重新初始化。Lua 数值按精确
微秒范围校验。脚本只有最后一条 SET 会修改状态，所有可能失败的校验与读取都在它之前，
避免误把脚本的原子可见性当成异常回滚能力。

租约剩余时间按第一次读取的 backend time 计算；客户端从调用开始计算保守期限并扣除完整
往返时间，因此不会把 CAS 等待时间额外加到租约上。直接调用 Driver 五个操作方法的调用者也必须遵守该约定。
Redis 服务器时钟仍是部署假设；这不是跨服务器时钟异常的全局时钟协议。

## 3. 只重试明确未提交的冲突

CAS/window miss 明确没有写入，可以在整体操作超时内以 1–10ms 有界退避重新读取。
NOSCRIPT 明确没有执行，可回退到 EVAL。其他网络/脚本错误返回零 Result：SET 可能成功，
但响应丢失，不能擅自重试，尤其 Renew 没有独立 operation ID，重试可能再次延长租期。

go-redis v9.22.0 的每命令 NoRetry 禁止不确定传输错误的内部重试，保留明确的 Cluster
MOVED/ASK 路由。构造器不修改共享客户端配置；要求 ContextTimeoutEnabled，拒绝副本读取及禁用 socket deadline 的配置。
原始 ReadTimeout/WriteTimeout=-1 仍保留 context deadline，可以使用；-2 会禁用底层
SetReadDeadline/SetWriteDeadline，不能仅凭 context 保证超时，因此构造时拒绝。
用户自行安装的 hooks 也不能偷偷重试未知写入。默认总操作超时 5 秒；ctx 更早的期限优先。
业务拒绝仍可能伴随维护状态提交，不能因为返回队满等错误而丢弃新快照。

## 4. 一个资源一个 Cluster slot

key 为 `airlock:{sha256(namespace + NUL + resource)}:state`。namespace/resource 禁止 NUL，
边界拼接无歧义，hash tag 保证该资源的读与发布属于同槽。不同资源能分布到不同节点，单个热点
资源不能靠 Cluster 自动拆分。注入客户端由调用者关闭，driver 构造不进行网络 I/O。

## 5. 永久 key 与部署约束

不设置 TTL：删除整个状态会同时删除活动授权、等待队列、调度游标和幂等历史。
必须使用 noeviction，禁止外部 DEL/EXPIRE/写入该 namespace；内存容量和永久资源数量由部署管理。
终态记录可按协议回收，整个资源的退休和重建需要另外的生命周期设计。

Redis 默认异步复制可能在切换后丢失已经确认的授权。持久化、备份恢复、主从切换与旧主隔离
不是 driver 自动完成的；WAIT 也不能把 Redis 变成强一致 CP 系统。业务进程暂停超过 TTL 后
仍可能继续运行；Lease.Done/Valid 需要业务遵守，租约不自动停止任务，也不提供通用 fencing。

## 6. 验证证明到哪里

默认 miniredis 测试覆盖共享契约、CAS 竞争、时间窗口、损坏状态、取消、续租和响应丢失注入；
真实 standalone 与三主 Cluster 运行共同契约、独立客户端和 SCRIPT FLUSH 路径。
真实 Cluster 是多节点路由/原子状态测试，不是 Cluster failover 保证。

`scripts/redis-failover.sh` 先同步一主一从，再暂停副本并断开复制连接，随后主库确认一个授权。
停止旧主后提升没有该写入的副本，验证在原租约仍有剩余时间时又能授予满容量。
这是刻意构造的异步复制缺失实验，预期结果是暴露局限，不是宣称故障切换安全，也不是 TCP 分区模拟。
脚本只操作自行创建的隔离容器、网络与临时文件。

参考：[Redis 脚本语义](https://redis.io/docs/latest/develop/programmability/eval-intro/)、
[复制和故障边界](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)、
[go-redis v9.22.0](https://github.com/redis/go-redis/tree/v9.22.0)。


实际验证记录（2026-10-23）：Go 1.24.13 / Darwin arm64；根模块覆盖率 100%，
Redis 生产文件覆盖率 97.22%，独立模块的 race/vet/lint 通过。真实 Redis 7.4 standalone、
Redis 7 三主 Cluster 契约测试，以及异步复制缺失 seed/verify 实验通过。
CI 增加 Redis 7/8 standalone、Cluster 和异步故障边界检查；这些是正确性验证，未给出吞吐或 P99 承诺。

审查后补充：构造校验拒绝禁用 socket deadline 的选项；默认和仅 context 超时配置均通过
net.Pipe 黑洞读写测试。Redis 模块最终在 race/覆盖率插桩下重复 5 轮通过；并发契约仍要求
24 个请求全部完成且容量从不超额，轮询间隔使用 50ms，不把此检查当作性能 SLA。

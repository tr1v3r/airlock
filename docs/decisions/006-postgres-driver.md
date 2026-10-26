# PostgreSQL driver 的重要取舍

更新：PG 的 snapshot 存储方案已由 [009 多表存储决策](009-postgres-relational.md) 取代；
driver 契约不限定持久化布局，结构化状态入口与快照入口都是可选辅助函数。以下保留原决策背景。

状态：本 PR 实现 PostgreSQL；依赖前一 PR 的 driver 契约与 memory。没有 tag/release。

## 每资源行锁与持久快照

每个 `(namespace, resource)` 对应一个永久 logged row。每次 Submit/Observe/Cancel/Renew/Release 都在短事务内创建缺失行、
`SELECT ... FOR UPDATE`、读取当前数据库时间、执行 protocol.ApplyCommand、写回结构化状态并 COMMIT。
不同资源不共享行锁；同资源所有客户端串行，不用 SKIP LOCKED 绕过 protected head。
业务执行期间不持连接或事务，连接池归调用者拥有，构造器不隐式运行 DDL。

第一版选择 `bytea` 中的版本化 JSON 快照，复用 core 状态机，而不是每种数据库再实现一套公平调度。
这偏离原提案的 resources/requests/leases 三表建议：不需要跨多表校验调度游标与计数，但每次操作
都读写全资源保留记录并产生 WAL，吞吐、行锁等待、存储膨胀和 vacuum 成本需要实测。
资源的历史记录数有上限，终态记录按契约回收；资源行/配置不自动删除。删除重建一个活跃资源会
丢失授权与幂等记录，不能作为日常 GC。正式生产应管理迁移、autovacuum 和服务端 statement timeout。

## 锁后取时与未知结果

`clock_timestamp()` 是拿到行锁之后的**另一条查询**。把它放在同一个 locking SELECT 的表达式里，
可能在等待锁之前求值；事务开始时固定的 `now()` 也不适用。续租不能因为等待旧锁而复活已过期授权。
参考 [PG 行锁](https://www.postgresql.org/docs/current/explicit-locking.html)和
[PG 时钟函数](https://www.postgresql.org/docs/current/functions-datetime.html)，核验日期 2026-10-23。

即使 ApplyCommand 返回队满等业务错误，也要提交非 nil 的新状态：它可能记录稳定拒绝、回收过期授权，
或把资源授予其他等待者。查询/写入/COMMIT 失败返回原错误链和零 Result，不能交付未知授权。
COMMIT 成功但响应丢失仍可能发生；以同 Request/LeaseID Observe、重试或 Cancel 恢复，不能新建 ID 重试。
操作有默认超时并尊重更早的 ctx；database/sql 无法强制一个忽略 cancellation 的底层 SQL driver 中断。

## 部署边界

schema/table 名严格按 SQL 标识符校验并引用，数据值全部绑定参数。namespace 与 resource 分列，
不使用容易碰撞的字符串前缀拼接。应用账号需要表操作权限；自动 EnsureSchema 另需 DDL 权限，
不建议所有生产实例并发运行迁移。不允许修改协议状态的触发器或外部写入者。

数据库行锁只提供当前权威数据库的原子性。可靠磁盘、fsync、同步复制、提交策略、备库选择与旧主隔离
属于部署前提。默认异步复制不能保证已确认授权在切换后保留。
测试采用 remote_apply 的同步备库，并在提升前停止旧主；结论只限于这一故障模型。
即便此模型保存授权，客户端暂停超过 TTL 后仍可能继续业务；仍需遵守 Lease.Done/Valid，不能把数据库
租约当成自动终止任务或通用 fencing。

## 验证范围

共同契约测试检查相同权重、组/租户公平次序、protected head、取消先到、幂等 GC 和租约边界。
PG 特有测试覆盖并发首次建资源、独立连接共享状态、锁等待后的过期、连接/取消错误以及写成功后丢响应恢复。
同步切换脚本在两次 Go 测试进程之间停止旧主并提升备库，随后验证原 lease 身份、protected head 与容量，
证明的是这一配置下已提交状态能恢复，不是任意网络分区或运维操作下不超发。


实际验证记录（2026-10-23）：Go 1.24.13 / Darwin arm64；PG17、PG18 单实例 race/shuffle 套件通过，
生产文件覆盖率 98.77%；PG16 Docker 同步 remote_apply 主备 seed/verify 两阶段 race 测试通过。
真实 PG 提交后在 database/sql 的 Tx.Commit 返回边界注入 EOF，随后第二连接查询与重试 Submit/Renew/Release
均保持原 lease/完整权重；这不是 TCP 分区模拟。另用 pg_terminate_backend 终止等待资源行锁的 session，
验证错误不返回授权且原授权仍保留。CI 固定运行 PG16/18 集成与 PG16 同步切换脚本。

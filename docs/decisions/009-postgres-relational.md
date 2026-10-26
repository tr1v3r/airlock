# PostgreSQL 多表存储的重要取舍

状态：实现于本 PR；替代 006 的整资源 snapshot 持久化。无 tag/release。

## 1. 契约与存储解耦

Driver 仍为 Submit/Observe/Cancel/Renew/Release，Shared/Lease 的用户调用不变。
底层不必使用参考状态机，也不必保存快照。本次将参考算法提取为结构化入口
（现名 protocol.ApplyCommand（*protocol.State, time.Time, Command）），JSON 编解码独立为快照适配，
memory/Redis 保留现有格式和路径。结构化入口要求独占输入状态，允许修改输入，不能与别的
操作共享 maps/records；返回 nil 不持久化，非 nil 即使伴随业务错误也要提交。

这样复用的是调度与生命周期算法，不是存储布局。protocol.State/protocol.Record（当时名为
DriverState/DriverRecord）是可选的核心辅助类型，现随状态机一起位于 protocol 包，其 Version 表示调度状态版本，不强制任何后端使用同名列。

## 2. 两张表，租约并入请求

resources 保存不可变 config、组内租户轮转 rotations、cursor、protected、revision、observed_ms。
config 与 rotations 使用 JSONB，前者是嵌套策略，后者只是组/租户顺序，不含请求/租约记录。
requests 将租户、权重、状态、身份、期限和 queue_position 拆为普通列；每个请求最多产生一个
租约，没有独立的租约生命周期，因此不引入第三张表及其连接、删除顺序。

主键分别为 (namespace,resource) 和 (namespace,resource,id)，通过外键关联。
FIFO 位置采用稳定 bigint 序号，读取按 tenant、queue_position 排序，队首出队不重写整条队列；
租户队列为空后可从零开始。序号耗尽时 fail closed，不回绕。
请求主键和租户队列唯一约束提供当前路径所需索引。没有额外建立全局到期/租户统计索引：
尚无全局后台清理与统计 API，后续按查询负载添加，避免每次续租承担无用索引维护。

## 3. 增量写入，不宣称增量读取

事务锁住资源后，仍读取最多 MaxRecords 个保留请求，构造结构化状态，运行纯调度算法；
读取设 10001 上限以识别超过协议最大 10000 的损坏状态。
计算前保留请求的值副本，计算后比较，只插入/更新变化的行，删除符合回收条件的终态行。
普通 Observe 更新资源 revision/time，不重写请求；单租约 Renew 不改写其他请求。

仍是 O(保留记录数) 的读取、内存与调度，热点资源也仍串行；不能仅凭拆表宣称更高吞吐。
变化较多时会有多条 SQL 和更多往返，未必比一次快照写更快。本 PR 验证持久化结构、写入范围
及契约正确性，没有给出吞吐改善百分比。后续热点优化需要实测，再考虑批量写入、索引驱动清理
或增量调度接口，不通过缩小扫描范围遗漏过期释放/公平顺序。

## 4. 保留资源行锁和锁后取时

READ COMMITTED 下先插入缺失资源行，再 SELECT FOR UPDATE，读取请求行，最后通过独立语句
取 clock_timestamp()。整个判定与多行写入在同一事务内完成，包含业务拒绝产生的维护变化。
不同资源不共享行锁；不会用 SKIP LOCKED 绕过 protected head。

不以拆表名义放开同一资源的并行判定，否则多个事务可能基于旧容量同时授予，或破坏公平顺序。
数据库操作不覆盖业务执行时间，不自动续租或执行 worker。未知提交结果继续返回零 Result，
后续以原请求身份恢复。部署持久性、同步复制和旧主隔离要求沿用 006。

## 5. 验证边界

复用 driverconformance 检查原子多令牌、取消先到、幂等重试、GC、保护队首、租户/组公平和期限。
PG 专项验证跨连接容量、锁等待后过期、连接终止、提交后响应丢失、重建后调度顺序。
新增真实 SQL 测试以请求行 xmin 检查无关行未重写、出队时队列顺序保留，以及对缺失必需列的
表结构的拒绝；单元故障注入覆盖多表读写的失败边界。
CI 延续 PG16/18 race 集成及同步主备切换；这不意味着任意网络分区或旧备份恢复都不超发。

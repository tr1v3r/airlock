# PostgreSQL driver

A durable adapter for Airlock's weighted, bounded, tenant/group-fair admission
protocol. Runtime dependencies are the standard library and Airlock core; the
application selects a `database/sql` PostgreSQL driver. Tests use pgx v5.

Start with the [Shared user guide](../../docs/guides/shared.md): resource, capacity
and queue limits are enough to configure admission; advanced protocol settings
have defaults. It includes a complete explicit-renewal, task-cancellation and
release-cleanup example. Backend implementation details below are for deployment
and adapter maintenance.


```go
namespace := "my-service"
driver, err := postgres.New(db, namespace,
    postgres.WithOperationTimeout(2*time.Second),
    postgres.WithTable("public.airlock_resources"))
// Handle err. New does not connect or execute DDL, and never closes db.
err = driver.EnsureSchema(ctx) // Explicit schema creation; handle err.
// Pass driver to airlock.NewShared with the shared resource configuration.
```

See the complete [example](example_test.go),
[driver contract](../../docs/decisions/005-driver-contract.md), and the PostgreSQL
tradeoffs in the repository's decisions directory. Multiple processes share
capacity only when they use the same database, table, namespace and resource.
The `namespace` argument to `New` is fixed for that driver. Namespace and resource
are separate columns, avoiding concatenation collisions.
Use the same immutable queue/lease policy for all clients; creation-capacity hints may differ.

Each command uses a short `READ COMMITTED` transaction: insert the resource if
absent, lock its row, read its request rows, read `clock_timestamp()` in a **subsequent statement**, apply
core `TransitionState`, update resource metadata and changed request rows, then commit. Backend time is read
after any lock wait, so an expired lease cannot be revived by a delayed renewal.
Business rejections still commit expiry and scheduling changes. Transport or
commit failures return no usable grant; a lost commit response is ambiguous and
must be recovered with the same request identity. There is no local fallback.

The default five-second operation bound includes pool and row-lock waits; a
shorter caller deadline wins. The SQL driver must honor cancellation and network
deadlines. Connections are returned before business execution. Polling is handled
by the core client; the adapter does not start workers, renew leases implicitly,
or hold a connection for the duration of an acquired lease.

`EnsureSchema` creates two **logged** tables atomically. Provision schemas yourself.
Custom names use quoted ASCII identifiers: schema up to 63 bytes, resource table
up to 54 bytes to leave room for the `_requests` suffix. Run deployment DDL
serially. Required columns are checked; this is not a comprehensive
schema/constraint audit. The only automatic migration relaxes the legacy
`CHECK (capacity > 0)` to `CHECK (capacity >= 0)`, preserving its actual constraint
name. Other layouts and constraints are unchanged.

**Upgrade before using Resize(0):** upgrade all clients to understand paused state,
then explicitly run the new `EnsureSchema` during deployment before publishing zero
capacity. Its transaction migrates the legacy capacity check on the configured
table, including custom schemas/tables. This DDL takes table locks; serialize it
with other deployment DDL. Operator-defined constraints beyond the recognized
legacy check require manual review. Request weight checks remain positive.

**使用 Resize(0) 前升级：** 先升级所有客户端，使其支持恢复暂停状态，再于部署时显式
运行新版 `EnsureSchema`，最后才发布零容量。它会在事务内将配置表上的旧
`CHECK (capacity > 0)` 迁为 `CHECK (capacity >= 0)`，保留实际约束名，支持自定义
schema/表。此 DDL 获取表锁，应与其他部署 DDL 串行执行。其他表结构/约束不变；
超出已识别旧约束的自定义约束需要人工检查。请求重量仍须为正数。

| Table | Key | Stored fields |
| --- | --- | --- |
| `public.airlock_resources` | `(namespace, resource)` | `state_version`, immutable-policy/creation-hint `config` JSONB, current `capacity`, `capacity_version`, tenant `rotations` JSONB, `protected`, `cursor`, `revision`, `observed_ms`, `next_maintenance_ms` |
| `public.airlock_requests` | `(namespace, resource, id)` | `owner`, `tenant`, `weight`, `submit_until_ms`, `wait_until_ms`, `status`, `lease_id`, `until_ms`, `finished_ms`, `failure`, `queue_position` |

The default tables are `public.airlock_resources` and `public.airlock_requests`.
Explicitly selecting `airlock_resources` with `WithTable` uses `airlock_requests`
in the selected schema as well. Other custom resource table names use the
`<resource_table>_requests` naming rule.

默认表名为 `airlock_resources` 和 `airlock_requests`。通过 `WithTable` 显式选择
`airlock_resources` 时也使用同 schema 下的 `airlock_requests`；其他自定义资源表名
追加 `_requests` 后缀。

See the exact [DDL](schema.go). Request lifecycle/lease fields are ordinary SQL
columns, not JSON. One request has at most one lease, so a third lease table would
add joins without an independent lifecycle. Request rows reference the resource;
a deferred unique constraint protects each tenant's FIFO position. Stable bigint
positions allow dequeue without renumbering all remaining waiters. Millisecond
instants are bigint epoch values; absent instants are SQL NULL. Revision uses
`numeric(20,0)` to preserve the full uint64 protocol range.
Current capacity uses a nonnegative `BIGINT` column; weight remains positive.
Creation capacity is positive; Resize accepts nonnegative Go `int` targets
(maximum `2^63-1` on 64-bit, `2^31-1` on 32-bit); scans reject values
outside the client's range. Charged sums are checked before restoring state or
publishing a projection. MaxRecords and queue limits still cap record counts,
not token weights.

当前容量列为非负 `BIGINT`，重量列仍为正数；创建容量须为正数，Resize 接受非负 Go `int`，
64 位最大 `2^63-1`、32 位最大 `2^31-1`。所有参与者应使用彼此能表示的范围，扫描拒绝超范围整数；恢复状态和构建观察
投影前检查占用总和溢出。记录数、队列数上限不随容量增大而放宽。

Read-only operational queries can use these columns (the result is not permission
to execute work):

```sql
SELECT tenant, status, sum(weight) AS tokens, count(*) AS requests
FROM public.airlock_requests
WHERE namespace = 'my-service' AND resource = 'export-jobs'
GROUP BY tenant, status;
```

Only configuration and group/tenant rotation lists remain JSONB on the resource
row. There is no complete-resource snapshot column. Mutations read all retained
requests for the resource (bounded by MaxRecords), run the shared scheduler, and
write only inserted/changed/deleted requests.

Ordinary Observe reads one resource header and one request by primary key in a
single MVCC statement, with no row lock, rotations load, full request scan or
metadata write. If the stored next-maintenance boundary is due, it obtains the
resource lock and rechecks using a new statement before loading full state; a
peer may already have completed maintenance. Lease/queue expiry and history
collection therefore progress even when no holder calls Release. Read-only
observations keep the current Revision; writes still serialize per resource.
Different resources lock different rows but share connection/backend capacity.

普通 Observe 按主键读取资源元数据和当前请求，不获取资源行锁，不扫描该资源的
全部请求，也不写入 Revision。维护到期后才进入资源锁，并在取得锁后重新查询，
合并已经由其他客户端完成的维护；无人 Release 时仍可通过租约到期推进调度。

Terminal records are reclaimed by subsequent operations, but resource rows are
permanent. Bound resource cardinality administratively. Stop all resource clients
and active work before deleting its request rows and resource row. Prohibit
external writes, protocol-changing triggers, unlogged/temporary tables, and
incompatible schema changes. Request rows contain owner secrets; restrict database
access. Tenant mapping is not authentication.

`New(db, namespace, options...)` 的第二个参数绑定 driver 的命名空间。
同一个 driver 可以管理多个独立 Resource；namespace 与 resource 分列存储，不改变表名。

终态请求由后续操作回收，资源行永久保留。需要管理资源总数，删除请求及资源行前必须停止所有相关
客户端及业务。禁止外部写入、改变协议的 trigger、unlogged/临时表和不兼容结构变更。
请求行含有 owner secret，应限制数据库访问；租户映射不提供身份认证。

持久性取决于 PostgreSQL 部署：开启可靠 WAL，根据要求使用同步复制，并在提升备库前
隔离旧主库。异步复制切换或恢复旧备份可能丢失已经确认的准入记录。本实现不负责选主或
外部 fencing。可选的 `scripts/postgres-failover.sh` 测试停止旧主库后提升同步备库。
租约过期只回收账本容量，不能停止业务；调用者必须响应租约丢失并独立保护外部写入。

测试命令见上文，真实数据库使用 `AIRLOCK_POSTGRES_DSN` 显式启用，应指定隔离测试库。
测试会创建两张表并保留唯一命名的资源行。覆盖通用协议、并发首次创建、跨连接容量、命名空间
隔离、重建 driver 后保留优先级游标、行锁等待后禁止复活、取消、连接失败，以及客户端退出
后 queued/granted 的回收。连接故障还通过终止正在等待行锁的测试 session 验证。丢失提交
响应使用可确定复现的单元模型及真实数据库提交成功后的响应丢失注入，不宣称模拟真实网络分区。

公开操作为 `Submit`、`Observe`、`Cancel`、`Renew` 和 `Release`。前三者接收不可变
`airlock.Request`；续租与释放接收 `airlock.LeaseRef{Request: original, ID: result.LeaseID}`，
均返回 `(airlock.Result, error)`。

Relational tests also verify request-row write isolation, queue-order preservation on dequeue, and rejection of schemas missing required columns.

多表专用测试还覆盖请求行写入隔离、出队时的队列顺序保留，以及对缺失必需列的表结构的拒绝。

## Resource management / 资源管理

ResourceManager.Resize reuses the resource row lock and transaction; capacity and
capacity_version live on airlock_resources and persist with request deltas.
InspectResource uses a single read-only MVCC statement, with no maintenance or
update lock. Resize accepts nonnegative targets; negative targets are invalid.
Zero pauses new grants while retaining waiters, fair order and the protected head;
new positive-weight acquires queue under normal budgets/deadlines, and existing
leases renew/release/expire normally. Resuming positive capacity rejects queued
weights above the target before fair dispatch, even for zero-to-positive expansion.
Creation stays positive; zero is not unlimited mode, and version CAS/no-op rules
are unchanged. See [Shared dynamic capacity](../../docs/guides/shared.md#shared-dynamic-capacity).

Resize 复用资源行锁事务，airlock_resources 的 capacity 与 capacity_version 随请求差异
一起提交。InspectResource 通过单条只读 MVCC 查询返回快照，不执行维护、不抢更新锁。
Resize 接受非负目标，负数无效。零暂停新增授予，保留等待者、公平顺序和保护队首；
新正权重申请按正常预算/截止时间排队，已有租约正常续期、释放、过期。
恢复正容量时，即使从零扩容，也先拒绝超重等待者再公平调度。创建容量仍为正数，
零不是无限模式，版本 CAS/同容量调用规则不变。完整语义见 [Shared 动态容量](../../docs/guides/shared.zh-CN.md#shared-动态容量)。

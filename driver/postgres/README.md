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
driver, err := postgres.New(db, "my-service",
    postgres.WithOperationTimeout(2*time.Second),
    postgres.WithTable("public.airlock_resources"))
// Handle err. New does not connect or execute DDL, and never closes db.
err = driver.EnsureSchema(ctx) // Explicit deployment migration; handle err.
// Pass driver to airlock.NewShared with the shared resource configuration.
```

See the complete [example](example_test.go),
[driver contract](../../docs/decisions/005-driver-contract.md), and the PostgreSQL
tradeoffs in the repository's decisions directory. Multiple processes share
capacity only when they use the same database, table, namespace and resource.
Namespace and resource are separate columns, avoiding concatenation collisions.
Use identical immutable `ResourceConfig` for all clients of that resource.

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
serially. Existing layouts are not automatically migrated; required columns are
checked, but this is not a comprehensive schema/constraint audit.

| Table | Key | Stored fields |
| --- | --- | --- |
| `public.airlock_resources` | `(namespace, resource)` | `state_version`, immutable `config` JSONB, tenant `rotations` JSONB, `protected`, `cursor`, `revision`, `observed_ms` |
| `public.airlock_resources_requests` | `(namespace, resource, id)` | `owner`, `tenant`, `weight`, `submit_until_ms`, `wait_until_ms`, `status`, `lease_id`, `until_ms`, `finished_ms`, `failure`, `queue_position` |

The request table is always `<resource_table>_requests`, in the same schema.
See the exact [DDL](schema.go). Request lifecycle/lease fields are ordinary SQL
columns, not JSON. One request has at most one lease, so a third lease table would
add joins without an independent lifecycle. Request rows reference the resource;
a deferred unique constraint protects each tenant's FIFO position. Stable bigint
positions allow dequeue without renumbering all remaining waiters. Millisecond
instants are bigint epoch values; absent instants are SQL NULL. Revision uses
`numeric(20,0)` to preserve the full uint64 protocol range.

Read-only operational queries can use these columns (the result is not permission
to execute work):

```sql
SELECT tenant, status, sum(weight) AS tokens, count(*) AS requests
FROM public.airlock_resources_requests
WHERE namespace = 'my-service' AND resource = 'export-jobs'
GROUP BY tenant, status;
```

Only configuration and group/tenant rotation lists remain JSONB on the resource
row. There is no complete-resource snapshot column. Each operation still reads
all retained requests for that resource (bounded by MaxRecords), runs the shared
pure scheduler, and writes only inserted/changed/deleted requests. Observe with
no maintenance changes updates the resource revision/time but no request rows;
renewing one lease does not rewrite unrelated requests. No throughput claim is
made: hot resources still serialize on one resource lock, and reads/scheduling
remain O(retained requests). Different resources lock different rows.

Terminal records are reclaimed by subsequent operations, but resource rows are
permanent. Bound resource cardinality administratively. Stop all resource clients
and active work before deleting its request rows and resource row. Prohibit
external writes, protocol-changing triggers, unlogged/temporary tables, and
incompatible schema changes. Request rows contain owner secrets; restrict database
access. Tenant mapping is not authentication.

### Upgrading the previous snapshot schema

This is a storage-format breaking change. Do not mix old and new binaries.
`EnsureSchema` rejects the old snapshot table instead of silently resetting state.
Back up the database, stop **all clients and active work across all namespaces in
these tables**, then run the explicit migration once:

```go
migration, err := postgres.New(db, "my-service",
    postgres.WithOperationTimeout(5*time.Minute))
// Handle err. Use the same WithTable option as the deployed driver, if customized.
err = migration.MigrateSnapshotSchema(ctx) // Handle err; ctx must allow enough time.
```

Migration takes an exclusive table lock and converts every namespace/resource in
one transaction. It preserves configuration, request/lease identities, deadlines,
terminal history, FIFO/group rotations, protected head, revision and observed time.
It never renews a lease. Errors before commit roll back both DDL and data; success
removes the old snapshot column. A lost COMMIT response is ambiguous: keep clients
stopped and inspect the schema/state before retrying or restoring. Rerunning it on the new layout fails. Verify with EnsureSchema,
then deploy only new clients. For rollback, stop clients/work again and restore the
backup; old binaries cannot read the new tables, and restoring an older backup
while clients are active is unsafe. This is not a rolling or zero-downtime upgrade.

Durability depends on PostgreSQL deployment settings. Use durable WAL settings,
synchronous replication as required, and fence an old primary before promotion.
Asynchronous replica promotion or restoring an older backup can lose acknowledged
admissions. This adapter does not implement failover election or external fencing.
The optional repository `scripts/postgres-failover.sh` tests a synchronous standby
promotion with the old primary stopped. Lease expiry reclaims accounting but
cannot stop business work; callers must stop on lease loss and protect external
writes independently.

```sh
GOWORK=off go test -race ./...  # Unit tests, no service required.
GOWORK=off go vet ./...
# Use an isolated test database; tests create the tables and retain unique rows.
AIRLOCK_POSTGRES_DSN='postgres://user:pass@127.0.0.1:5432/test?sslmode=disable' \
  GOWORK=off go test -race -count=1 ./...
```

Real-backend tests run the common conformance suite and check concurrent initial
creation, cross-connection capacity, namespace isolation, restart-persistent
priority cursor, renewal after a row-lock wait, cancellation, connection failure,
and reclamation of abandoned queued/granted requests. Connection failure is also
exercised by terminating a blocked test session. Lost commit responses are checked
with a deterministic unit model and post-commit response-loss injection against a
real database; these are not network-partition experiments.

The public operations are `Submit`, `Observe`, `Cancel`, `Renew`, and `Release`.
The first three accept an immutable `airlock.Request`; renewal and release use
`airlock.LeaseRef{Request: original, ID: result.LeaseID}`. All return `(airlock.Result, error)`.

## 中文

业务接入先看 [Shared 使用指南](../../docs/guides/shared.zh-CN.md)：只需配置资源、容量与队列，
高级协议参数已有默认值。指南提供显式续租、任务取消和释放清理的完整示例；下面的存储细节
供部署和后端维护参考。

PostgreSQL driver 提供持久化的多令牌、租户/优先级公平、有界排队协议，运行时只依赖标准库
和 Airlock core。应用自行选择 `database/sql` 驱动并管理连接池；测试使用 pgx v5。
上面的构造示例和完整 example 可直接参考：`New` 不连接、不执行 DDL、不关闭连接池；
`EnsureSchema` 是显式的部署步骤。多个进程共享容量时，数据库、表、namespace、resource
必须一致，且使用相同的不可变 `ResourceConfig`。namespace 与 resource 分列存储，避免
前缀拼接碰撞。

每次命令使用短 `READ COMMITTED` 事务：首次插入资源行、行锁、读取请求行、在**后续独立语句**获取
`clock_timestamp()`、调用 core `TransitionState`、增量保存资源信息和变化的请求行并提交。这样续租等待行锁后
会重新检查数据库时间，不能复活已过期租约。业务错误仍提交过期清理和调度变化；连接或
提交错误不返回可用 grant。丢失提交响应意味着结果不确定，需用同一请求身份恢复，不会
退化为本地准入。

默认 5 秒操作超时包括连接池及行锁等待，调用者更短的 deadline 优先；SQL 驱动必须支持
取消和网络超时。事务结束便归还连接，业务执行期间不占用连接。轮询由 core 客户端负责，
适配器不执行 worker、不自动续租，也不在整个租约期间持有连接。

`EnsureSchema` 在一个事务中创建两张普通 logged 表，不创建 schema、不自动升级旧结构。
它检查必需列是否存在，不是完整的类型/约束审计。自定义 schema 名最多 63 字节，资源表名最多
54 字节，为请求表的 `_requests` 后缀留空间；只接受 ASCII 标识符，逐段加引号。生产 DDL 应串行执行。

默认表结构见上面的字段表与 [完整 DDL](schema.go)：

- `airlock_resources`：每资源一行，保存配置、租户轮转顺序、protected head、组游标、修订号和后端时间。
- `airlock_resources_requests`：每请求一行，租户、权重、状态、租约 ID、期限与 FIFO 位置均为普通列。
  请求和租约是一对一生命周期，因此先合并存放；外键关联资源行，延迟唯一约束保护租户内的 FIFO 位置。
  FIFO 位置使用稳定的 bigint 序号，队首出队不会重新编号全部后续请求。

时间列用毫秒 epoch bigint，缺失时间为 NULL；修订号使用 numeric(20,0)，保留 uint64 全范围。
仅配置和组/租户轮转列表仍为 JSONB，已经没有整份资源 snapshot 列。可以直接 SQL 按租户/状态查询，
但查询结果不是业务执行许可，也不能直接编辑协议记录。

每次操作仍读取该资源全部有界保留记录，执行共享纯调度算法，再只写入新增、变化或删除的请求。
没有调度变化的 Observe 只更新资源行；单租约续租不重写无关请求。
**这不代表吞吐已优化完成**：同一资源仍通过一把行锁串行处理，读取及调度成本仍为 O(保留请求数)。
不同资源锁不同的行，实际负载下的吞吐和 vacuum/WAL 成本需要实测。

终态请求由后续操作回收，资源行永久保留。需要管理资源总数，删除请求及资源行前必须停止所有相关
客户端及业务。禁止外部写入、改变协议的 trigger、unlogged/临时表和不兼容结构变更。
请求行含有 owner secret，应限制数据库访问；租户映射不提供身份认证。

### 从旧 snapshot 表升级

这是存储格式不兼容变更，不支持新旧客户端混跑。`EnsureSchema` 会拒绝旧表，不会清空或重置状态。
备份数据库，停止**这组表所有 namespace 的客户端与活跃业务**，再按上面的 Go 示例显式调用一次
`MigrateSnapshotSchema(ctx)`，设置足够长的操作超时和 ctx deadline，并保持部署时的 WithTable 配置。

迁移获得整表独占锁，在同一事务中转换所有 namespace/resource，保留配置、请求/租约身份、期限、
终态历史、FIFO/组轮转、protected head、修订号和时间，不延长租约。提交前失败会同时回滚 DDL 和数据；成功后删除旧 snapshot 列。
如果 COMMIT 响应丢失，结果仍可能已提交：保持停服，先核验表结构和数据，再决定重试或恢复。对新表重复执行会报错。完成后用 EnsureSchema 核验并仅启动新客户端。
如需回退，重新停止所有客户端及业务后恢复备份；旧版本不能读取新表，活跃期间恢复旧备份也不安全。
不支持滚动升级或零停机迁移。

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

Relational tests also verify request-row write isolation, multi-namespace migration, preserved lease deadlines/queue order, and rollback on corrupt legacy data.

多表专用测试还覆盖请求行写入隔离、多 namespace 迁移、租约期限与队列顺序保留，以及旧数据损坏时的整体回滚。

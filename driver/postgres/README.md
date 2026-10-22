# PostgreSQL driver

A durable adapter for Airlock's weighted, bounded, tenant/group-fair admission
protocol. Runtime dependencies are the standard library and Airlock core; the
application selects a `database/sql` PostgreSQL driver. Tests use pgx v5.

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
absent, lock its row, read `clock_timestamp()` in a **subsequent statement**, apply
core `Transition`, write the returned snapshot and commit. Backend time is read
after any lock wait, so an expired lease cannot be revived by a delayed renewal.
Business rejections still commit expiry and scheduling changes. Transport or
commit failures return no usable grant; a lost commit response is ambiguous and
must be recovered with the same request identity. There is no local fallback.

The default five-second operation bound includes pool and row-lock waits; a
shorter caller deadline wins. The SQL driver must honor cancellation and network
deadlines. Connections are returned before business execution. Polling is handled
by the core client; the adapter does not start workers, renew leases implicitly,
or hold a connection for the duration of an acquired lease.

`EnsureSchema` creates this **logged** table. Provision schemas yourself; custom
names accept only quoted ASCII identifier components, each at most 63 bytes.
Prefer a serialized migration: concurrent `CREATE TABLE IF NOT EXISTS` can still
race in PostgreSQL's catalogs. Existing tables are not validated or migrated.

```sql
CREATE TABLE public.airlock_resources (
    namespace text COLLATE pg_catalog."C" NOT NULL,
    resource text COLLATE pg_catalog."C" NOT NULL,
    snapshot bytea NOT NULL,
    PRIMARY KEY (namespace, resource)
);
```

Snapshots use `bytea` so the core serialization is stored verbatim. Each resource
has one permanent row, including its configuration and scheduling cursor. Terminal
request records are reclaimed by later operations, but resource rows are never
removed automatically. Bound resource cardinality administratively; deleting a
resource requires stopping **all** its clients and active work first. Prohibit
external writes, triggers that alter the protocol, unlogged/temporary tables and
incompatible schema changes. Limit database access: snapshots contain owner
secrets, and tenant mapping is not authentication.

One hot resource serializes through one row and rewrites its whole bounded
snapshot. Independent resources lock different rows. This favors transactional
correctness and shared semantics over high-cardinality queue throughput; measure
under your queue sizes and poll rates before deployment.

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
# Use an isolated test database; tests create the table and retain unique rows.
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

PostgreSQL driver 提供持久化的多令牌、租户/优先级公平、有界排队协议，运行时只依赖标准库
和 Airlock core。应用自行选择 `database/sql` 驱动并管理连接池；测试使用 pgx v5。
上面的构造示例和完整 example 可直接参考：`New` 不连接、不执行 DDL、不关闭连接池；
`EnsureSchema` 是显式的部署步骤。多个进程共享容量时，数据库、表、namespace、resource
必须一致，且使用相同的不可变 `ResourceConfig`。namespace 与 resource 分列存储，避免
前缀拼接碰撞。

每次命令使用短 `READ COMMITTED` 事务：首次插入资源行、行锁、在**后续独立语句**获取
`clock_timestamp()`、调用 core `Transition`、保存返回快照并提交。这样续租等待行锁后
会重新检查数据库时间，不能复活已过期租约。业务错误仍提交过期清理和调度变化；连接或
提交错误不返回可用 grant。丢失提交响应意味着结果不确定，需用同一请求身份恢复，不会
退化为本地准入。

默认 5 秒操作超时包括连接池及行锁等待，调用者更短的 deadline 优先；SQL 驱动必须支持
取消和网络超时。事务结束便归还连接，业务执行期间不占用连接。轮询由 core 客户端负责，
适配器不执行 worker、不自动续租，也不在整个租约期间持有连接。

`EnsureSchema` 创建上面展示的普通 logged 表，不创建 schema，不验证或迁移已有表。
自定义表名仅接受每段最多 63 字节的 ASCII 标识符，并逐段加引号。生产环境应串行迁移，
并发 `CREATE TABLE IF NOT EXISTS` 仍可能在 PostgreSQL 系统目录冲突。快照使用 `bytea`
原样保存。每个资源永久保留一行及其配置、调度游标；请求终态记录由后续命令回收，但不
自动删除资源行。需要管理资源总数，删除资源前必须停止所有相关客户端及业务。
禁止外部写入、改变协议的 trigger、unlogged/临时表和不兼容的结构变更。数据库快照含有
owner secret，应限制访问权限；租户映射本身不提供身份认证。

单个热点资源通过一把行锁串行处理，每次重写整个有界快照；不同资源锁不同的行。这以
事务正确性和统一语义为优先，实际队列长度、轮询频率下的吞吐需要自行测量。

持久性取决于 PostgreSQL 部署：开启可靠 WAL，根据要求使用同步复制，并在提升备库前
隔离旧主库。异步复制切换或恢复旧备份可能丢失已经确认的准入记录。本实现不负责选主或
外部 fencing。可选的 `scripts/postgres-failover.sh` 测试停止旧主库后提升同步备库。
租约过期只回收账本容量，不能停止业务；调用者必须响应租约丢失并独立保护外部写入。

测试命令见上文，真实数据库使用 `AIRLOCK_POSTGRES_DSN` 显式启用，应指定隔离测试库。
测试会创建表并保留唯一命名的资源行。覆盖通用协议、并发首次创建、跨连接容量、命名空间
隔离、重建 driver 后保留优先级游标、行锁等待后禁止复活、取消、连接失败，以及客户端退出
后 queued/granted 的回收。连接故障还通过终止正在等待行锁的测试 session 验证。丢失提交
响应使用可确定复现的单元模型及真实数据库提交成功后的响应丢失注入，不宣称模拟真实网络分区。

公开操作为 `Submit`、`Observe`、`Cancel`、`Renew` 和 `Release`。前三者接收不可变
`airlock.Request`；续租与释放接收 `airlock.LeaseRef{Request: original, ID: result.LeaseID}`，
均返回 `(airlock.Result, error)`。

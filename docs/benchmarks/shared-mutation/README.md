# Shared mutation cost / Shared 修改路径成本

Historical experiment: the later fresh-layout simplification removes the old
full/delta projection writer benchmarks. Use commit `08c366f` to access those
historical benchmark fixtures; the commands below are not current-layout benchmarks.

Baseline: `2a522e05e08223f19d381025986cea572ac7b479`. Candidate: the code and
benchmarks accompanying this document. Measurements use Go 1.27.3, darwin/arm64,
Apple M3 Pro (12 logical CPUs), PostgreSQL 18 Alpine and Redis 8 Alpine in dedicated
Docker containers with localhost port forwarding. These are local samples, not
throughput commitments or latency percentiles. No production traffic is used.

## Projection construction

`BenchmarkProjectionUpdate` compares full projection encoding with a one-record
delta at fixed history sizes. Delta timing includes building the lifecycle baseline
map and decoding the old metadata, not just encoding the changed record. Fixture
construction is excluded. The same retained records are used by both paths; their
short fixture IDs make the absolute byte counts smaller than production IDs.
Three runs of 30 iterations each are recorded in `projection.txt`; table entries
are the median run (rounded).

| Records | Full time | Delta time | Full allocation | Delta allocation | Full payload | Delta payload |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 6.37 µs | 6.65 µs | 4.32 KB | 6.19 KB | 1,127 B | 1,096 B |
| 1,000 | 2.48 ms | 0.107 ms | 5.20 MB | 0.252 MB | 638,273 B | 1,096 B |
| 10,000 | 23.15 ms | 1.73 ms | 52.65 MB | 1.98 MB | 6,398,273 B | 1,096 B |

The small-resource case has extra metadata/baseline overhead. The larger-history
benefit comes from encoding and publishing only changed projections, while keeping
unchanged generations intact. This does **not** eliminate the full snapshot read,
encode, validation or scheduler passes. Overall mutations remain O(retained records).

增量路径计时包含旧元数据解码与基线 map 分配；单记录有额外开销，大历史场景减少重复
编码与写入。完整快照和调度状态验证仍是 O(保留记录数)，不能把投影收益等同于端到端收益。

## Redis end-to-end renewal

The existing real Redis `BenchmarkIntegrationRenewWithWaiters` was rerun sequentially
for baseline then candidate, with 50 timed operations per case. Setup and teardown
are excluded; retained history/queue size is fixed during each timed sample.

| Waiters | Before | After | Before allocation | After allocation |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0.94 ms | 1.21 ms | 27.14 KB | 26.69 KB |
| 1,000 | 19.34 ms | 11.27 ms | 12.19 MB | 6.69 MB |

These are single local runs with host/container and scheduling variability. The
1,000-waiter sample shows about 42% lower time and 45% less allocation; the small
case does not show a latency improvement. No p95/p99, concurrent throughput or
failover claim follows from these samples. Raw files are `redis-renew-before.txt`
and `redis-renew-after.txt`.

真实 Redis 基准按基线、候选顺序串行运行，每项 50 次。1,000 等待者样本的总续租时间
约下降 42%、分配约下降 45%；1 等待者未观察到延迟改善。这是本机单次样本，不代表
生产吞吐、尾延迟或故障恢复性能。

## PostgreSQL batches

`TestPersistBatchesBoundParametersAndCheckCounts` verifies that 513 deletes and
513 upserts use 3 statements each, instead of 513 each. Every batch is capped at
256 records (3,584 bind parameters for upserts). `TestIntegrationBatchHistoryCollection`
and `TestIntegrationBatchExpiryReusesQueuePosition` exercise 513-row collection and
expiry/new-queue-position reuse against PostgreSQL, including the deferred unique
constraint and resource transaction boundary.

The existing single-row `BenchmarkIntegrationRenewWithWaiters` was also sampled
(20 iterations, one run per version; raw outputs included). With 1,000 waiters,
allocation was approximately 2.40 MB/op both before and after. Those timing samples
were taken during other verification work and must not be used to claim a latency
speedup or regression. Batching targets bulk changes, not single-row renewal.

513 条删除或 upsert 从 513 次 SQL 往返降为 3 次；真实 PostgreSQL 验证了跨批到期、GC
和队列位置复用。单行续租的 1,000 等待者样本分配前后均约 2.40 MB/op，时间样本受并行
验证影响，仅保留原始数据，不据此宣称延迟提升或退化。

## Reproduce

From the repository root with a workspace containing root and driver modules:

```sh
go test -run '^$' -bench BenchmarkProjectionUpdate -benchtime=30x -count=3 ./driver/redis/...
AIRLOCK_REDIS_ADDR=127.0.0.1:6379 go test -run '^$' \
  -bench 'BenchmarkIntegrationRenewWithWaiters/(1|1000)$' -benchtime=50x ./driver/redis/...
AIRLOCK_POSTGRES_DSN='postgres://airlock:airlock@127.0.0.1:5432/airlock?sslmode=disable' \
  go test -race -run TestIntegrationBatch ./driver/postgres/...
```

Real Redis Cluster, Redis failover and synchronous PostgreSQL failover were not
rerun for this change. Standalone Redis and PostgreSQL integration suites passed.

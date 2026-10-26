# Shared observation benchmark / Shared 观察基准

The benchmarks compare one queued Observe against a fully occupied resource with
1, 100 or 1000 queued requests. The holder and queues have one-hour lifetimes,
so these measurements exclude expiry, GC and new grants. Fixture creation,
schema setup and cleanup are outside the timed loop. RenewWithWaiters measures
the holder's renewal against the same queues to expose write-path costs.

基准比较容量占满、分别有 1/100/1000 个排队请求时的一次 Observe；另测持有者 Renew，
展示维护索引的写入代价。租约和排队有效期为一小时，计时不包含到期清理、授予、建表、
构造队列和清理数据。

- Baseline: cd3fc4dc882018c436897c6a803411b9c2191dc7 (dev before this change).
- Candidate: the implementation and benchmark definitions in this PR. The same
  benchmark functions were copied to the baseline checkout.
- Linux/arm64 Go 1.27.0 test binaries, Docker on macOS; PostgreSQL 18 Alpine,
  Redis 8 Alpine; 12 reported CPUs, default GOMAXPROCS.
- The binary ran **inside its backend container**, using loopback and the same
  VM clock. Host/VM clock skew also reproduced failures on unmodified dev;
  running inside the container removes that environmental mismatch.
- Each case has exactly 100 timed operations, one recorded run per version, sequential
  before/after execution for each backend. These are raw microbenchmarks, not
  statistically established speedups, production throughput or latency SLAs.
  No p95/p99, concurrent waiter load, hot-resource interference, failover or
  write/read mix is inferred from these numbers.

## Queued Observe

| Backend | Queued | Before ns/op | After ns/op | Before B/op | After B/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| PostgreSQL | 1 | 1300396 | 55588 | 18587 | 6328 |
| PostgreSQL | 100 | 3111654 | 124405 | 235918 | 6342 |
| PostgreSQL | 1000 | 7879299 | 144637 | 2398515 | 6409 |
| Redis | 1 | 73885 | 27143 | 17033 | 5381 |
| Redis | 100 | 1057481 | 18898 | 545696 | 5377 |
| Redis | 1000 | 12426740 | 20187 | 7041745 | 5401 |

The constant-size request projection removes record-count-dependent transfer,
decoding and scheduler restoration from ordinary polling. Configuration size
still matters: the compact header includes the immutable policy, and request
validation can inspect its tenant mappings.

普通轮询不再随保留请求数增长而传输、解码和恢复整个调度状态；配置大小仍有影响，
观察头包含不可变策略，校验也会检查租户映射。

## Write tradeoff / 写入取舍

PostgreSQL mutations still load retained requests and hold the resource-row lock.
Redis mutations still read/encode/write the complete snapshot, and additionally
maintain per-request hash fields. The index uses extra memory and write/encoding
work. This is a read-heavy waiting optimization, not an incremental scheduler or
a general optimization of all operations. Raw Renew results are included beside
the Observe results; compare them before choosing a backend or polling interval.

PG 修改仍需全量读取并持有资源行锁。Redis 修改仍处理整份快照，且需要额外维护请求
索引，增加内存、编码和写入成本。不能把这里的轮询收益推广到只有 Submit/Renew/Release
而没有大量等待查询的场景。每个等待者仍默认每 50ms 查询，调用次数没有减少。

## Reproduce / 复现

Use isolated services, no replica routing or eviction, and the same Go version.
From the corresponding driver module:

~~~sh
AIRLOCK_POSTGRES_DSN='postgres://airlock:airlock@127.0.0.1:5432/airlock?sslmode=disable' \
  go test -run '^$' -bench 'BenchmarkIntegration(ObserveQueued|RenewWithWaiters)' -benchtime=100x

AIRLOCK_REDIS_ADDR=127.0.0.1:6379 \
  go test -run '^$' -bench 'BenchmarkIntegration(ObserveQueued|RenewWithWaiters)' -benchtime=100x
~~~

For cross-OS Docker runs, compile with GOOS=linux GOARCH=arm64 CGO_ENABLED=0
go test -c in the module, copy the binary into the dedicated container, then
pass the equivalent -test.run, -test.bench and -test.benchtime flags.
Set the architecture to the container's actual architecture.

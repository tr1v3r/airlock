# Airlock

[English](README.md) | [简体中文](README.zh-CN.md)

仅依赖标准库的 Go 并发限制器，支持通过 context 取消等待，需要 Go 1.24+。
池只负责控制准入；goroutine、结果、错误和完成等待均由调用方管理。
不维护 worker 或可执行任务队列，也不启动后台 goroutine；Gate 只排队准入请求。

## 迁移状态

当前基线迁自 `tr1v3r/pkg` 提交
`b3e47bdfd27a149ecb4da01580b91be277b905f6` 的 `pools/`。
保留 `NewPool`、`Acquire(ctx)`、`Release()`、`Num()` 和 `Size()`，
每次获取一个名额；加权获取仅为后续方向，尚未实现。
`main` 分支保留基础实现，后续开发在 `dev` 分支进行。
下方 breaking change 部分为继承自 pools 的历史说明。

## 按租户准入的 Gate

只需要总并发上限时使用 `Pool`。共享容量且需要租户内 FIFO、等待租户间轮转及显式排队上限时，
使用 `Gate`。两者**每次均只申请一个令牌**，尚不支持多权重请求。

```go
gate, err := airlock.NewGate(airlock.GateConfig{
    Capacity:           20,
    MaxQueued:          100,
    MaxQueuedPerTenant: 10,
})
if err != nil {
    return err
}
permit, err := gate.Acquire(ctx, tenantID)
if err != nil {
    return err
}
defer permit.Release()
// 执行业务，另外把 ctx 传给业务代码。
```

该片段位于返回 error 的函数中。完整可运行代码见[租户示例](examples/tenants/example.go)。

- 容量必须为正数，队列上限非负，单租户上限不能超过全局上限；否则 `NewGate` 返回
  `ErrInvalidGateConfig`。队列上限为零表示不允许等待。所有上限创建后固定。
- 租户 ID 为精确匹配的非空字符串。nil context 会 panic；空 ID 在检查取消之前返回
  `ErrInvalidTenant`。调用方应提供可信且长度受限的 ID，Gate 不负责身份验证或字符串长度限制。
- 无法立即获取时，队列已满分别返回 `ErrQueueFull` 或 `ErrTenantQueueFull`；同时触发时优先返回
  全局错误。已取消的 context 在检查容量和队列之前返回其错误。支持 `errors.Is`，失败时不返回 Permit。
- 非空租户队列轮流放行，租户内部 FIFO。新等待租户加入轮转队尾；队列清空即移除，重新活跃时再次加入队尾。
- 队列上限只统计尚未授予名额的请求。其他租户空闲时，单个租户可使用全部执行容量。不会抢占已运行任务。
  公平不代表执行时间相同或延迟有上界；全局队列满时，新租户仍可能被拒绝。
- 取消和授予串行决定：取消先完成则不保留令牌；授予先完成则成功返回，即使随后取消也需要释放。
  取消准入等待不会停止已经获得名额的业务。
- 仅使用返回的 `*Permit`，禁止值复制。`Release` 并发安全且幂等，nil/零值 Permit 释放无操作；不会自动归还名额。
- `Stats()` 返回一致的 `GateStats` 快照：`Capacity`、`InUse`、`Queued`、`WaitingTenants`，不能作为任务完成屏障。
  Gate 必须通过 `NewGate` 创建、禁止复制，内部不启动 goroutine。

[设计记录](docs/proposals/001-tenant-fair-admission.md) 描述具体契约。
[历史原型测量](docs/proposals/001-experiment-results.md) 尚未证明优于表现最好的两级 semaphore，
也不构成生产延迟保证。比较负载现已接入公开 Gate API；`x/sync` 仍仅用于测试。

```sh
GOWORK=off AIRLOCK_LOAD=1 go test -v ./internal/comparison -run '^TestLoadComparison$' -count=1
GOWORK=off go test ./internal/comparison -run '^$' -bench BenchmarkAdmission -benchmem -count=3
```

## 安装

```sh
go get github.com/tr1v3r/airlock
```

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "log"
    "sync"

    "github.com/tr1v3r/airlock"
)

func main() {
    p := airlock.NewPool(2)
    var wg sync.WaitGroup
    results := make([]int, 5)
    for i := range results {
        if err := p.Acquire(context.Background()); err != nil {
            log.Fatal(err)
        }
        wg.Add(1)
        go func() {
            defer wg.Done()
            defer p.Release()
            results[i] = (i + 1) * (i + 1)
        }()
    }
    wg.Wait()
    fmt.Println(results) // [1 4 9 16 25]
}
```


先获取令牌再启动 worker，能够约束 worker 的创建数量。每个 worker 写入独立结果槽位，
在 `wg.Wait` 后读取；共享 map 和并发 append 仍需另行同步。请求处理时应传入请求的 context。
如果提交途中获取失败，应等待已启动的 worker 再返回；
[批量示例](examples/batch/example.go) 用 `defer wg.Wait()` 演示了这一点。

## API

```go
type Pool interface {
    Acquire(ctx context.Context) error
    Release()
    Num() int
    Size() int
}
```

- `NewPool(size)`：正数限制并发；负数不限并发；零直接 panic。
  必须通过构造函数创建，零值不是公开 API。
- `Acquire(ctx)`：等待可用容量，或返回 context 错误。不能传 nil context。
  `context.Background()` 表示显式选择无截止时间的等待。
  已取消的 context 会直接被拒绝，即使当前有容量或处于无限并发模式。
- 每次成功获取后必须恰好调用一次 `Release()`，通常使用 `defer`；获取失败时不能释放。
  成功后的取消不会归还令牌，也不会停止任务；任务本身需要另行接收 context。
- 取消与容量可用同时发生时，两种结果都可能出现；以返回值决定是否需要释放令牌。
- 无限并发模式下 `Release()` 不做任何操作。有界模式下，无令牌可释放时 panic。
  令牌没有所有者标识，无法识别误释放其他 worker 的令牌，调用方仍须保证正确配对。
- `Num()` 返回有界池已持有令牌数，不含等待请求；`Size()` 返回容量。
  无限并发模式下两者都返回零。它们不表示任务完成状态。
- 不需要 `Close`。通过 `sync.WaitGroup` 等待任务，在启动 worker 前调用 `Add`；
  对空闲的 WaitGroup，开始完成等待前应先登记任务。池不恢复 panic，也不汇总错误。

## 取消等待

在返回 `error` 的函数中，假设已有池 `p` 和上游 context `ctx`：

```go
ctx, cancel := context.WithTimeout(ctx, time.Second)
defer cancel()
if err := p.Acquire(ctx); err != nil {
    return err // 未获取令牌，不调用 Release。
}
defer p.Release()
// 执行任务。
```

需导入 `context` 和 `time`。`Acquire` 在当前 goroutine 中等待，不额外启动等待 goroutine。
需要异步执行时由调用方启动并管理 goroutine；通常先获取再启动，可以对任务提交施加背压。

## 示例与验证

在 `airlock/` 目录运行：

| 示例 | 命令 |
| --- | --- |
| [有界批量处理与有序结果](examples/batch/example.go) | `go run ./examples/batch` |
| [获取令牌超时](examples/cancellation/example.go) | `go run ./examples/cancellation` |
| [调用方管理完成通知](examples/completion/example.go) | `go run ./examples/completion` |
| [按租户的单令牌准入](examples/tenants/example.go) | `go run ./examples/tenants` |

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

## Breaking change 迁移

| 移除的 API | 替代方案 |
| --- | --- |
| `Wait()` | 调用 `Acquire(ctx)` 并处理错误；无限期等待使用 `context.Background()`。 |
| `WaitContext(ctx)` | `Acquire(ctx)`。 |
| `Done()` | `Release()`。 |
| `AsyncWait()` | 直接调用 `Acquire(ctx)`，或自行管理 goroutine 及返回结果。 |
| `WaitAll()` | 调用方使用 `sync.WaitGroup`。 |
| `AsyncWaitAll()` | 调用方启动 goroutine 等待 `sync.WaitGroup`，完成后关闭 channel。 |

自定义实现和 mock 需适配新的 `Pool` 接口。零容量现在会 panic。
无限并发和有界模式都不再跟踪任务完成，批次完成统一由调用方管理。
有界池无令牌可释放时现在会 panic，而非阻塞。保留 `Num` 和 `Size`。

# Airlock

[English](README.md) | [简体中文](README.zh-CN.md)

面向 Go 1.24+ 的多令牌、按租户并发准入库。
提供租户内 FIFO、等待租户间轮转，以及全局和每租户排队上限。
运行时仅依赖标准库；goroutine、结果、错误和任务完成由调用方管理。

## 安装

```sh
go get github.com/tr1v3r/airlock
```

此 API 正在面向 `dev` 的功能分支开发，尚无包含该 API 的标签版本。

## Hello world

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/tr1v3r/airlock"
)

func main() {
	lock, err := airlock.New(airlock.Config{Capacity: 2})
	if err != nil {
		log.Fatal(err)
	}
	permit, err := lock.Acquire(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer permit.Release()
	fmt.Println("hello, airlock")
}
```

`Acquire(ctx)` 使用一个共享的默认租户。只配置 `Capacity` 时，容量满了立即返回
`ErrQueueFull`。需要等待时，必须设置**两个**队列上限，例如
`Config{Capacity: 2, MaxQueued: 8, MaxQueuedPerTenant: 8}`。
默认租户遵循相同的排队上限、FIFO 和取消规则，不会与任何显式租户 ID 冲突。

## 用法与契约

```go
lock, err := airlock.New(airlock.Config{
    Capacity:           20,
    MaxQueued:          100,
    MaxQueuedPerTenant: 10,
})
if err != nil {
    return err
}
permit, err := lock.AcquireFor(ctx, tenantID)
if err != nil {
    return err
}
defer permit.Release()
// 执行业务，另外把 ctx 传给业务代码。
```

该片段位于返回 error 的函数中。完整可运行代码见[租户示例](examples/tenants/example.go)。
多令牌工作可将申请改为 `lock.AcquireNFor(ctx, tenantID, 3)`，一次预留三个令牌，
仍通过 `permit.Release()` 一次归还全部三个。无需租户 ID 时使用 `lock.AcquireN(ctx, 3)`。
完整代码见[多令牌示例](examples/weighted/example.go)。

- 容量必须为正数，队列上限非负，单租户上限不能超过全局上限；否则 `New` 返回
  `ErrInvalidConfig`。队列上限为零表示不允许等待。所有上限创建后固定。
- `Acquire(ctx)` 和 `AcquireFor(ctx, tenantID)` 申请一个令牌。
  默认租户通过 `AcquireN(ctx, n)`、指定租户通过 `AcquireNFor(ctx, tenantID, n)`
  原子申请全部 `n` 个令牌，等待时不会占有部分容量。显式租户 ID 必须为非空字符串。
  调用方应提供可信且长度受限的 ID，Airlock 不负责身份验证或字符串长度限制。
- nil context 会 panic，然后检查显式租户 ID（`ErrInvalidTenant`），再检查权重：
  `n <= 0` 返回 `ErrInvalidWeight`，`n > Capacity` 返回 `ErrWeightExceedsCapacity`。
  非法权重立即失败、不入队，即使 context 已取消也优先返回权重错误。
- 无法立即获取时，队列已满分别返回 `ErrQueueFull` 或 `ErrTenantQueueFull`；同时触发时优先返回
  全局错误。已取消的 context 在检查可用容量和队列之前返回其错误。支持 `errors.Is`，失败时不返回 Permit。
- 非空租户队列轮流放行，租户内部 FIFO。有可用容量时，调度器选择下一个租户的队首请求，
  消耗该租户的一次轮转机会。如果容量不足以容纳完整权重，该请求受保护，后续的小请求或新请求
  不能绕过。取消已选请求不会返还轮转机会。容量完全占满时不提前选择请求。
- 新等待租户加入轮转队尾；队列清空即移除，重新活跃时再次加入队尾。公平按选择机会计量，
  不按令牌数或执行时间计量。保护大请求可能闲置令牌并延迟其他租户；避免无限插队的前提是
  已持有 Permit 最终释放，且调度持续推进。
- 队列上限按等待请求数计量，不按权重计量。其他租户空闲时，单个租户可使用全部令牌容量。
  不会抢占已运行任务。
  公平不代表执行时间相同或延迟有上界；全局队列满时，新租户仍可能被拒绝。
- 取消和授予串行决定：取消先完成则不保留令牌；授予先完成则成功返回，即使随后取消也需要释放。
  取消准入等待不会停止已经获得名额的业务。
- 仅使用返回的 `*Permit`，禁止值复制。`Release` 并发安全且幂等，只归还一次完整申请权重；
  nil/零值 Permit 释放无操作。不支持部分释放或调整 Permit 权重。
  这是进程内凭证，没有租约、自动过期或自动归还。
- `Stats()` 返回一致的 `Stats` 快照：`Capacity`、`InUse`、`ActivePermits`、`Queued`、`WaitingTenants`。
  `Capacity` 和 `InUse` 按令牌计数，`ActivePermits` 是未释放凭证数，`Queued` 是等待请求数。
  单令牌调用中 `InUse == ActivePermits`。快照不能作为任务完成屏障。
  Airlock 必须通过 `New` 创建、禁止复制，内部不启动 goroutine。

[本地多令牌设计取舍](docs/decisions/001-local-weighted-tradeoffs.md)记录大请求保护、
按请求机会公平和显式释放等选择的理由、代价与重新评估条件。

[初始设计记录](docs/proposals/001-tenant-fair-admission.md) 描述单令牌基础，
[多令牌契约](docs/proposals/002-weighted-scheduling.md) 在此基础上扩展；其中优先级分组仍为提案。
[历史原型测量](docs/proposals/001-experiment-results.md) 尚未证明优于表现最好的两级 semaphore，
也不构成生产延迟保证。比较负载现已接入公开 Airlock API；`x/sync` 仍仅用于测试。

```sh
GOWORK=off AIRLOCK_LOAD=1 go test -v ./internal/comparison -run '^TestLoadComparison$' -count=1
GOWORK=off go test ./internal/comparison -run '^$' -bench BenchmarkAdmission -benchmem -count=3
```

## 示例与验证

在 `airlock/` 目录运行：

| 示例 | 命令 |
| --- | --- |
| [Hello world](examples/hello/example.go) | `go run ./examples/hello` |
| [有界批量处理与有序结果](examples/batch/example.go) | `go run ./examples/batch` |
| [获取令牌超时](examples/cancellation/example.go) | `go run ./examples/cancellation` |
| [调用方管理完成通知](examples/completion/example.go) | `go run ./examples/completion` |
| [按租户的单令牌准入](examples/tenants/example.go) | `go run ./examples/tenants` |
| [多租户多令牌准入](examples/weighted/example.go) | `go run ./examples/weighted` |

```sh
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

## CI 质量门禁

PR、推送到 `dev`/`main`、合并队列、手动触发和每周扫描均运行
[CI](.github/workflows/ci.yml)。所有任务必须成功，最终的 **CI Gate** 才会通过：

- `gofmt`、`go vet` 和固定版本的 golangci-lint 检查正确性、错误处理、
  导出 API 注释、命名、拼写，并要求 lint 豁免注明具体检查项和原因。
  质量检查固定使用 Go 1.27.0 与 golangci-lint v2.13.2，需配套升级并验证，
  避免分析器尚未支持新版 Go 的导出格式。兼容性测试和漏洞扫描仍跟踪稳定版 Go。
- 检查模块清单整洁性、校验和，并保证运行时仅依赖标准库。
- Linux 覆盖 Go 1.24、1.25、上一稳定版及稳定版，macOS/Windows 覆盖稳定版。
  测试禁用缓存并随机排序，`GOTOOLCHAIN=local` 防止自动升级工具链掩盖最低版本不兼容。
  每个矩阵任务都会执行所有文档示例。
- Linux 运行 race 检测，并在 CPU 数 1/2/4 下各重复十轮乱序测试。
  原子语句覆盖率要求每个生产文件及总计均达到 **95%**（当前核心基线为 **100%**）。
  示例和仅用于比较的代码不计入覆盖率；新增生产包自动纳入。
  覆盖率报告保存 14 天，失败时也上传。
- 固定版本的 `govulncheck -test` 阻止 SDK 和测试中可达的已知漏洞；这不代表不存在其他漏洞。
- 校验工作流语法并测试覆盖率检查器。Action 固定完整提交 SHA，只授予读取权限，
  配置任务超时，PR 不使用密钥。Dependabot 每周提出 Action/模块更新；
  工作流中固定的 CLI 工具版本需单独审查更新。

本地等效检查（需要 Go、Python 3、Bash 和对应固定版本的工具）：

```sh
bash scripts/check.sh format
bash scripts/check.sh modules
GOWORK=off go vet ./...
golangci-lint config verify && golangci-lint run
actionlint
python3 -m unittest discover -s scripts -p 'test_*.py'
GOWORK=off go test -shuffle=on -count=1 -timeout=5m ./...
bash scripts/check.sh coverage
GOWORK=off go test -race -shuffle=on -count=10 -cpu=1,2,4 -timeout=5m ./...
bash scripts/check.sh examples
GOWORK=off govulncheck -test ./...
```

`modules` 会运行 `go mod tidy`，模块清单变化时失败；本地清单可能被更新，供审查。
工具安装命令和版本见工作流。覆盖率统计可执行语句，不代表分支覆盖率或正确性；
不要仅为通过 CI 降低阈值或屏蔽问题。共享 runner 缺少稳定的性能基线，
性能比较保留为按需运行。API 兼容性检查需要先约定已发布的 API 基线，目前尚未强制执行。

**仓库设置：** 推送并运行工作流后，需要为 `dev`/`main` 配置分支规则，
要求 **CI Gate** 成功、通过 PR 和审查批准，并按需禁止绕过与强推。
仅提交工作流不能阻止合并；本次变更不修改远端仓库规则。每周定时任务运行默认分支的工作流。

## 迁移

唯一的准入类型为 `Airlock`，通过 `New(Config)` 构造。
每次 `Acquire(ctx)` 或 `AcquireFor(ctx, tenantID)` 仍然**仅获取一个令牌**。
新增 `AcquireN(ctx, n)` 和 `AcquireNFor(ctx, tenantID, n)` 获取 `n` 个令牌，
通过 `Permit.Release()` 一次归还。无限模式、动态容量、优先级分组和分布式 driver 尚未实现。

这是开发阶段的破坏性变更：移除 `Pool`/`NewPool` 和先前的 `Gate`/`NewGate` 名称。
原调用方需要显式配置队列上限，可选择传入租户 ID，保存返回的 Permit 并释放它。
`Stats().InUse`、`Stats().Capacity` 分别替代 `Num()`、`Size()` 的快照用途。
仅需要简单总并发上限、不需要租户准入时，可直接使用 `x/sync/semaphore`。不提供兼容别名。

默认租户使用 `Acquire(ctx)`，指定租户使用 `AcquireFor(ctx, tenantID)`。
原来的 `Acquire(ctx, tenantID)` 调用需迁移到 `AcquireFor`；接口、mock 和方法值类型
也需使用对应的固定参数签名。四个入口都使用固定参数。
`Stats.ActivePermits` 新增独立的凭证计数；升级时请将不带字段名的 `Stats` 字面量改为带字段名的写法。

原 channel 策略只保留为测试基线。初始代码迁自 `tr1v3r/pkg` 提交
`b3e47bdfd27a149ecb4da01580b91be277b905f6` 的 `pools/`，Git 历史保留原实现。
任务完成由调用方通过 `sync.WaitGroup` 等待；后续获取失败时也应等待之前启动的任务。
Airlock 不执行任务或跟踪完成状态。

## 设计提案

[多令牌、优先级准入与可选 driver 提案](docs/proposals/002-weighted-priority-drivers.md)描述本地、Redis 和 PostgreSQL 方案。本分支实现阶段 1：本地原子多令牌申请、租户轮转与队首保护。
优先级分组和分布式 driver 仍为提案；这不代表已经合入 `dev` 或发布标签版本。

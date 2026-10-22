# Airlock

支持权重与取消的 Go 并发限制器。

[English](README.md)

Airlock 是独立的新项目，探索支持 context 的加权并发准入控制：任务按成本
申请容量，并在完成后归还。名称来自“气闸舱”，表达受控准入的含义。

## 当前状态

目前仅完成仓库初始化，尚未实现限制器，也没有稳定的公开 API。
与 `github.com/tr1v3r/pkg/pools` 分开演进。

## 预期 API（尚未实现）

- `Acquire(ctx, n)`：原子获取全部申请容量；失败时不占用容量。
- `Release(n)`：归还之前获取的容量。

实现前需明确取消、非法权重、排队公平性与超容量申请的语义。
任务执行与完成等待由调用方负责。

## 开发

要求 Go 1.24 或更高版本，模块路径为 `github.com/tr1v3r/airlock`。

```sh
go test ./...
go vet ./...
```

## 许可证

[MIT](LICENSE)。

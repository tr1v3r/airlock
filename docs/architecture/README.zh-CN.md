# Airlock 架构图集

[English](README.md) | [简体中文](README.zh-CN.md) | [打开图集导航](index.html)

这套图解释已实现的准入机制，并把读者引向可运行示例、源码与回归测试。
图中文字以中文为主，API 名称保留英文；英文导读覆盖同样的入口与边界。

## 怎么看

- **使用者**：01 整体架构 → 02 本地准入；使用共享容量时继续 05 租约 → 08 应用监督。
- **初学者**：01 → 02 → 03，先理解容量、等待请求、租户和 permit 的区别；再看 04–06 的分布式协议。
- **开发者**：03 调度 → 04 状态机 → 07 驱动；PG 继续看 09–10，Redis 看 11；按下方需求表找到实现和测试。

GitHub 文件页不会执行 HTML。下方 PNG 可直接预览；克隆仓库后，用浏览器打开
`docs/architecture/index.html`，或下载单张 HTML 后打开。HTML 内联了图和查看器，
无需服务端；支持搜索、节点聚焦、浅深主题及 PNG/SVG 导出。
PNG 是静态浏览器预览，不替代可探索的 HTML。01–08 的源码引用固定到
[`ca393a3`](https://github.com/tr1v3r/airlock/tree/ca393a3031c310c21515cec04e98b4f009f50e08)。
本次刷新对齐 `dev` 的重命名（`shared.go`、`shared_state.go`、`driver_transition.go`）、
本地 `Resize` 与只读观察；图中的源码行号属于固定快照，不是浮动的 `dev`。
PG 视图 09–10 保留 [`cbdfca7`](https://github.com/tr1v3r/airlock/tree/cbdfca745233d8cd3a67fd7d236c76d13d07f436)。Redis 视图 11 对齐 `dev` 的 [`f9de5e5`](https://github.com/tr1v3r/airlock/tree/f9de5e5e79bd2d06d97b0eba4ce45dbbb99b6410)，包含 Resource 更新锁与 owner/CommitWindow 校验。每张候选 JSON 单独记录版本，其他视图保留原快照。

## 按需求切入

| 需求 | 使用入口 / 修改位置 | 验证重点 |
| --- | --- | --- |
| 限制单进程在途工作 | [hello](../../examples/hello/example.go)、`New(Config)` | 成功后释放；队列零值不等待 |
| 运行期调整容量 | [airlock.go](../../airlock.go)、`Resize` | 收缩不撤销已授 Permit；超重等待者失败 |
| 批量原子占用容量 | [weighted](../../examples/weighted/example.go)、`AcquireNFor` | 不部分授予；大请求保护 |
| 忙时立即拒绝 | [tryacquire](../../examples/tryacquire/example.go)、`TryAcquireNFor` | 不插队；仍获取 mutex |
| 租户优先级与等待预算 | [priority](../../examples/priority/example.go)、[priority.go](../../priority.go) | 按请求选择计量；不保证 token 份额 |
| 高低优先级容量隔离 | [isolation](../../examples/isolation/example.go) | 独立实例、固定分区；不是借用或抢占 |
| 多实例共享容量 | [Shared 使用指南](../guides/shared.zh-CN.md)、`NewShared` | 资源配置一致；租约不等于强制停止 |
| 管理续租与任务停止 | [lifecycle 示例](../../driver/memory/examples/lifecycle/main.go) | 先停止任务及监督，再释放；未知结果只重试清理 |
| 新增后端 | [自定义驱动指南](../guides/drivers.zh-CN.md)、[Driver](../../driver.go) | 后端时间、原子提交、幂等、未知结果；运行 conformance |
| 修改调度 / 状态机 | [airlock.go](../../airlock.go)、[shared_state.go](../../shared_state.go) | 本地与共享复用调度；同时检查 model / race 测试 |

## 图集

### 01 · Airlock：两条准入路径与责任边界

从 New 还是 NewShared 开始？谁执行任务？

[HTML](.archify/architecture-overview-20261024-152114/overview.html) · [JSON](.archify/architecture-overview-20261024-152114/candidate.json)

![Airlock：两条准入路径与责任边界](previews/overview.png)

实现：[airlock.go](../../airlock.go), [shared.go](../../shared.go), [driver.go](../../driver.go)

测试：[airlock_test.go](../../airlock_test.go), [shared_test.go](../../shared_test.go)

### 02 · 本地准入：快速路径、排队与取消竞争

为什么申请失败？取消与授予同时发生怎么办？

[HTML](.archify/workflow-local-admission-20261024-152114/local-admission.html) · [JSON](.archify/workflow-local-admission-20261024-152114/candidate.json)

![本地准入：快速路径、排队与取消竞争](previews/local-admission.png)

实现：[airlock.go](../../airlock.go), [try_acquire.go](../../try_acquire.go)

测试：[weighted_test.go](../../weighted_test.go), [try_acquire_test.go](../../try_acquire_test.go)

### 03 · 调度器：组配额、租户轮转与受保护请求

为什么还有空闲 token 却不能放行？

[HTML](.archify/workflow-scheduler-20261024-152114/scheduler.html) · [JSON](.archify/workflow-scheduler-20261024-152114/candidate.json)

![调度器：组配额、租户轮转与受保护请求](previews/scheduler.png)

实现：[priority.go](../../priority.go), [airlock.go](../../airlock.go), [examples/isolation/example.go](../../examples/isolation/example.go)

测试：[priority_test.go](../../priority_test.go), [local_model_test.go](../../local_model_test.go)

### 04 · 后端请求生命周期：状态、幂等与回收

哪些状态能续租、取消和回收？

[HTML](.archify/lifecycle-request-states-20261024-152114/request-states.html) · [JSON](.archify/lifecycle-request-states-20261024-152114/candidate.json)

![后端请求生命周期：状态、幂等与回收](previews/request-states.png)

实现：[shared_state.go](../../shared_state.go), [driver.go](../../driver.go)

测试：[shared_state_test.go](../../shared_state_test.go), [shared_state_property_test.go](../../shared_state_property_test.go)

### 05 · 客户端 Lease：有效期与不可逆失效

本地失效是否代表后端已经释放？

[HTML](.archify/lifecycle-lease-states-20261024-152114/lease-states.html) · [JSON](.archify/lifecycle-lease-states-20261024-152114/candidate.json)

![客户端 Lease：有效期与不可逆失效](previews/lease-states.png)

实现：[shared.go](../../shared.go)

测试：[shared_concurrency_test.go](../../shared_concurrency_test.go), [shared_clock_test.go](../../shared_clock_test.go)

### 06 · Shared 调用链：排队、观察与租约交付

排队后是谁推进状态？何时才能运行任务？

[HTML](.archify/sequence-shared-sequence-20261024-152114/shared-sequence.html) · [JSON](.archify/sequence-shared-sequence-20261024-152114/candidate.json)

![Shared 调用链：排队、观察与租约交付](previews/shared-sequence.png)

实现：[shared.go](../../shared.go), [driver/postgres/postgres.go](../../driver/postgres/postgres.go), [driver/redis/redis.go](../../driver/redis/redis.go)

测试：[shared_test.go](../../shared_test.go), [shared_concurrency_test.go](../../shared_concurrency_test.go)

### 07 · 驱动实现：原子边界与快照持久化

自定义驱动必须在哪里保证原子性？

[HTML](.archify/architecture-driver-boundaries-20261024-152114/driver-boundaries.html) · [JSON](.archify/architecture-driver-boundaries-20261024-152114/candidate.json)

![驱动实现：原子边界与快照持久化](previews/driver-boundaries.png)

实现：[driver.go](../../driver.go), [driver_transition.go](../../driver_transition.go), [driver/postgres/postgres.go](../../driver/postgres/postgres.go), [driver/redis/scripts.go](../../driver/redis/scripts.go)

测试：[internal/driverconformance/conformance.go](../../internal/driverconformance/conformance.go), [driver/redis/commit_test.go](../../driver/redis/commit_test.go)

### 08 · 应用监督流程：停止任务后再释放租约

任务、续租和释放应该按什么顺序停止？

[HTML](.archify/workflow-application-lifecycle-20261024-152114/application-lifecycle.html) · [JSON](.archify/workflow-application-lifecycle-20261024-152114/candidate.json)

![应用监督流程：停止任务后再释放租约](previews/application-lifecycle.png)

实现：[driver/memory/examples/lifecycle/main.go](../../driver/memory/examples/lifecycle/main.go)

测试：[driver/memory/examples/lifecycle/main_test.go](../../driver/memory/examples/lifecycle/main_test.go)

### 09 · PG 操作流程：无锁观察与事务更新

五类业务操作何时加锁？读写哪张表？Resize/Inspect 复用同一事务边界（见图 12）。

[HTML](.archify/workflow-pg-operations-20261028-0245/pg-operations.html) · [JSON](.archify/workflow-pg-operations-20261028-0245/candidate.json)

![PG 操作流程：无锁观察与事务更新](previews/pg-operations.png)

实现：[driver/postgres/postgres.go](../../driver/postgres/postgres.go), [driver/postgres/observe.go](../../driver/postgres/observe.go), [driver/postgres/storage.go](../../driver/postgres/storage.go), [driver/postgres/resource.go](../../driver/postgres/resource.go), [shared_state.go](../../shared_state.go)

测试：[driver/postgres/postgres_test.go](../../driver/postgres/postgres_test.go), [driver/postgres/storage_test.go](../../driver/postgres/storage_test.go), [driver/postgres/postgres_integration_test.go](../../driver/postgres/postgres_integration_test.go)

### 10 · PG 表结构：资源、请求与复合键

资源行与请求行如何关联？每个字段负责什么？

[HTML](.archify/erd-pg-tables-20261027-140038/pg-tables.html) · [JSON](.archify/erd-pg-tables-20261027-140038/candidate.json)

![PG 表结构：资源、请求与复合键](previews/pg-tables.png)

实现：[driver/postgres/schema.go](../../driver/postgres/schema.go), [driver/postgres/storage.go](../../driver/postgres/storage.go), [driver/postgres/observe.go](../../driver/postgres/observe.go)

测试：[driver/postgres/postgres_test.go](../../driver/postgres/postgres_test.go), [driver/postgres/storage_test.go](../../driver/postgres/storage_test.go)

### 11 · Redis 操作流程：更新锁与 owner/CommitWindow 校验

Hash 和更新锁如何协作？Observe 何时抢锁？

[HTML](.archify/workflow-redis-operations-20261028-0245/redis-operations.html) · [JSON](.archify/workflow-redis-operations-20261028-0245/candidate.json)

![Redis 操作流程：更新锁与 owner/CommitWindow 校验](previews/redis-operations.png)

实现：[driver/redis/redis.go](../../driver/redis/redis.go), [driver/redis/observe.go](../../driver/redis/observe.go), [driver/redis/resource.go](../../driver/redis/resource.go), [driver/redis/scripts.go](../../driver/redis/scripts.go), [driver/redis/update_lock.go](../../driver/redis/update_lock.go), [driver_transition.go](../../driver_transition.go), [shared_state.go](../../shared_state.go)

测试：[driver/redis/redis_test.go](../../driver/redis/redis_test.go), [driver/redis/commit_test.go](../../driver/redis/commit_test.go), [driver/redis/integration_test.go](../../driver/redis/integration_test.go), [shared_commit_window_test.go](../../shared_commit_window_test.go)

### 12 · Shared 资源管理：Inspect 与 Resize

Inspect 读到哪里？Resize 的版本语义如何在 PG 行锁事务与 Redis 更新锁/Lua 提交中落地？

[HTML](.archify/sequence-resource-management-20261028-0245/resource-management.html) · [JSON](.archify/sequence-resource-management-20261028-0245/candidate.json)

![Shared 资源管理：Inspect 与 Resize](previews/resource-management.png)

实现：[shared_resource.go](../../shared_resource.go), [shared_state.go](../../shared_state.go), [driver/postgres/resource.go](../../driver/postgres/resource.go), [driver/redis/resource.go](../../driver/redis/resource.go)

测试：[shared_resource_test.go](../../shared_resource_test.go), [internal/driverconformance/resource.go](../../internal/driverconformance/resource.go)

## 阅读边界

- Shared 版本化 Resize/Inspect 由图 12（时序）与更新后的 PG/Redis 操作图卡片覆盖；其余图仍为各自 pinned 版本的源码快照。

- 图展示代码行为，不代表测得的吞吐或 SLA；时序图的纵轴只表示顺序。
- 后端状态图保留完整状态转换，交叉线较多；可在 HTML 中聚焦状态逐条阅读。常见的重复观察、自循环等无状态变化操作省略。
- 客户端 Lease 图单独描述本地有效性。失效不等于后端已释放，终态也无法强制结束应用工作。
- 驱动图展示 I/O 所有权；Memory / PostgreSQL 调用 Transition 的关系写在图下卡片中。Redis 校验 Transition 返回的 CommitWindow；免维护期各驱动都可只读应答 Observe。
- 应用监督图来自可运行示例；`runJob` 不是 SDK API。获取失败不启动业务，监督取消与业务完成的先后取决于实际执行。
- PG 图展示当前完整状态读取与请求差异写入；Redis 图展示单 Hash 的权威 `state`、派生 `meta`/`r:<id>`、Resource 更新锁及原子的 owner/CommitWindow 校验，不再比较旧快照。#50/#51 增量转换仍是提案。
- 提案、尚未合入的分支、部署拓扑和性能瀑布图未被当作已实现架构。

## 维护与验证

编辑 `manifest.json` 指向的 `candidate.json`，保留与对应源码版本一致的引用。
复现和发布前检查见 [维护说明](MAINTAINING.md)。[验证清单](verification.json) 记录 HTML 与输入哈希和自动化结果。

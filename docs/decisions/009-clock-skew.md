# ADR 009: explicit client-local clock-skew allowance

Status: implemented in this change; no protocol version or resource migration.

## Why

`Shared` used the entire configured SubmitWindow and MaxWait when constructing
absolute deadlines from the client wall clock. `Transition` correctly rejects a
new request whose deadlines exceed its backend-time windows. Consequently, even
a small stable client-ahead offset greater than command delivery time could make
every fresh acquisition fail with `ErrInvalidRequest`. Increasing the windows
alone does not remove that offset. The review reproduced this with a backend
100ms behind the client; aligned and backend-ahead controls succeeded.

Removing the backend bounds is not acceptable: they constrain admission and
retained recovery history. Rewriting a request's deadline after submission is
also not acceptable: the request ID binds its immutable SubmitUntil and prevents
resurrection after terminal history is collected.

## Decision

Add `SharedOptions.ClockSkewAllowance` (`time.Duration`) as an opt-in **client-local**
setting. It is not part of ResourceConfig; callers of one resource may use different
allowances while sharing the same resolved immutable resource policy.

- Zero is the default and exactly preserves the existing deadline construction.
  It does not auto-detect or compensate for skew.
- A positive allowance `A` must be a whole number of milliseconds and satisfy
  `2*A < SubmitWindow` and `2*A < MaxWait`, after normal defaults are resolved.
  Invalid values return `ErrInvalidConfig` before backend I/O. Validation compares
  bounds before subtracting, rather than multiplying arbitrary durations by two.
- Construct `SubmitUntil = floor_ms(clientNow + SubmitWindow - A)`.
- Construct `WaitUntil = floor_ms(min(clientNow + MaxWait, callerDeadline) - A)`;
  omit callerDeadline when absent. The ID is created from the adjusted SubmitUntil.
- With a positive allowance, if the adjusted waiting deadline has already passed
  locally, return `ErrLeaseLost` without contacting the driver or attempting
  cleanup. An already cancelled caller still receives its context error. This
  error means there is no conservative waiting budget, not that the caller's
  actual context deadline has necessarily elapsed. Zero retains the old path.

The operator chooses `A` from a known deployment offset bound. It is not a universal
constant: silently adding 100ms would change existing short-deadline callers and
would not guarantee anything about deployments with larger skew.

For clocks within `±A`, the effective backend windows can be as short as
`window - 2*A`, before transport/processing delay is deducted. This explains the
strict two-sided validation; it does not promise an acquisition succeeds. Earlier
caller deadlines are deliberately shortened too and may still expire on the
backend despite passing local preflight.

## Preserved boundaries and failure behavior

Backend command validation, absolute-ID binding, stable rejection, cancellation
tombstones, retention/GC and no-resurrection behavior are unchanged. So are
conservative lease Remaining/round-trip accounting, explicit renewal, no automatic
fresh-identity retries and no local fallback. This change does not add lease time.
An insufficient allowance can still produce rejection or expiry, never authority
to work alongside an error. The allowance is neither a skew detector nor clock
synchronization. Arbitrary clock jumps, unbounded network delay/process pauses and
fencing of external side effects remain outside its guarantee.

## Regression strategy

- Stable backend offsets in both directions; aligned clocks and zero-allowance
  compatibility controls; repeated rejection with insufficient allowance.
- Default/explicit allowance resolution, whole-millisecond and strict half-window
  boundaries, negative/fractional/maximum-duration inputs and overflow safety.
- Earlier caller deadlines, local budget exhaustion without I/O, cancelled-context
  precedence, immutable deadline-bearing identities and cancelled grants that
  cannot revive on retry.
- Shared clients with different allowances still interoperate on one resource.
- Complementary concurrent weighted admission, queued cancellation, uncertain
  renew/release responses, delayed renewal after local expiry, and terminal-state
  drainage tests. `AIRLOCK_STRESS=1` increases bounded concurrent workloads; this
  is not a throughput SLA or a simulation of arbitrary distributed failures.

## 中文摘要

新增客户端本地参数 `SharedOptions.ClockSkewAllowance`，默认零值不改变旧行为，也不自动
校准时钟。部署方按已知偏差上界显式选择预算 `A`；正值须为整毫秒，并在默认值补齐后满足
`2*A < SubmitWindow` 和 `2*A < MaxWait`。两个绝对截止都扣除预算，包括调用者更早的
等待 deadline；ID 根据调整后的提交截止生成。

正预算耗尽本地等待余量时，不调用后端，返回 `ErrLeaseLost`；已经取消的 ctx 仍返回自身
错误。这表示没有保守等待预算，不是宣称 ctx 已过期。预算为零时不新增这条早退路径。
偏差在 `±A` 内时，实际后端窗口最短可为 `window - 2*A`，还需扣除网络/处理耗时。

该参数不进入 ResourceConfig，不改变后端校验、ID 绑定、墓碑/GC、不复活规则、租约
Remaining/RTT 计算或不自动重试的约定。预算不足仍可能拒绝/过期；不保证任意时钟跳变、
网络延迟或进程暂停下的可用性，不提供 fencing。对应双语用户指南与定向回归测试同步维护。

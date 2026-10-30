# Partitioned Shared history

[简体中文](partitioned-history.zh-CN.md)

Redis and PostgreSQL keep retained terminal outcomes outside the state restored
for normal admission scheduling. Driver, Shared and the complete-state Transition
remain compatible; TransitionPartial is an optional storage helper.

## Storage

This is the sole development layout, with scheduler state version 1. There is
no v1/v2 negotiation, automatic migration, mixed-writer mode or downgrade path.
Use fresh, disposable state while developing; existing development data is not
upgraded. Never erase live holders or idempotency records merely to change layout.

Redis `state` contains the active-only JSON snapshot. The same hash has `meta` and
`r:*` request projections; a same-slot sorted set at `stateKey:history` indexes
terminal collection deadlines. PostgreSQL creates the request/resource schema
with the count and deadline columns directly, and loads active rows plus the target
terminal row. EnsureSchema creates/checks this layout; it does not upgrade an old
schema for this feature. Existing deployment DDL restrictions still apply.

## Semantics and publication

The partial transition receives every queued/granted record and the complete
addressed terminal, even if due for GC: request-conflict detection still precedes
collection. Omitted history count is exact; its earliest collection time bounds
publication. Collection remains max(Finished+Retention, SubmitUntil). MaxRecords
counts both active and retained terminal records.

Expiry, cancellation/release, terminal outcomes, capacity reuse and queue dispatch
commit under the existing resource serialization boundary. PostgreSQL uses one
transaction. Redis atomically checks update-lock owner and CommitWindow; before
multi-command writes it empties state/meta, then publishes both together as its
last write. Empty state in an existing resource is incomplete, not a new resource.
This is failure protection, not a format-version marker. Redis runtime errors do
not roll back writes; an incomplete publication remains unavailable. Stop affected
writers and drain/stop their business work before controlled integrity recovery;
do not reset the resource or blindly replay the uncertain mutation.

All due history may still be collected in one operation. Active state is still
restored; Renew does not bypass fairness or maintenance. Unrelated terminal payloads
are validated on write and target reads rather than audited in every operation;
protect storage against out-of-band edits. Complete audits remain separate.
Backend durability limits and application lease/fencing obligations still apply.

# Partitioned Shared history

[简体中文](partitioned-history.zh-CN.md)

Redis and PostgreSQL keep retained terminal outcomes outside the state restored
for normal admission scheduling. `Driver`, `Shared`, local admission and the
complete-state `Transition` contract retain their behavior. Custom drivers may
continue using complete snapshots. `TransitionPartial` is an optional lower-level
helper for stores that atomically maintain authoritative history metadata.

## Atomicity and retention

A partial transition includes every queued/granted record and the addressed
request's complete existing record, even if that terminal record is due for GC.
Request identity conflicts are checked before collection, as in a full transition.
The omitted terminal count is exact; it excludes records already collectible at
backend evaluation time. The earliest omitted collection deadline bounds commits.
Collection time is `max(Finished + Retention, SubmitUntil)`.

Active expiry, release/cancellation, terminal outcome publication, capacity reuse,
queue dispatch and resource metadata commit under one resource serialization
boundary. No asynchronous history write or automatic replay of uncertain writes
is introduced. A deadline or lost response still cannot prove no mutation occurred.
`MaxRecords` continues to count both active and retained terminal records.

Unrelated live terminal records no longer need transfer, decode or scheduler
restoration on every operation. All due history may still be collected in one
operation: this step bounds work by active state and due history, not by a fixed
GC time budget. Active state itself is still restored/validated; Renew is not an
independent row update that bypasses admission maintenance or fairness.

## Storage and deployment

Redis stores an active-only snapshot in `active`, request projections in `r:*`,
and terminal collection deadlines in a same-slot sorted set at `stateKey:history`.
The `state` field is a v2 marker. The update lease owner and CommitWindow remain
checked at atomic publication. Multi-command publication first marks the resource
incomplete, so a runtime error cannot expose partially moved records as valid.

PostgreSQL keeps the request table, adds indexed collection deadlines and an exact
resource count, and loads the active rows plus the target terminal row. Updates
and indexed collection remain in one transaction under the resource row lock.
Run `EnsureSchema` during controlled deployment; schema/index DDL may block writes.

An existing v1 resource is fully read and validated once, then migrated on its
first committed transition, including maintenance or a business-error transition
with a nonnil next state. That initial operation is still proportional
to history and can need a larger deployment budget. Subsequent mutations use the
partitioned format. Read-only observations may leave a v1 resource unmigrated.

Upgrade all writers before the first v2 mutation and serialize schema changes.
Old full-state writers cannot safely manage v2 resources and must fail closed.
Do not treat rollback to an old binary as a storage downgrade. A downgrade needs
an application-owned, offline validated conversion with writers stopped; this
change does not provide one. Never delete resource keys/rows or shorten retention
to force rollback, as that can forget owners, capacity or idempotency outcomes.

## Integrity boundaries

The store validates migrated records, writes, active records, addressed terminal
records and index/count consistency checks. It no longer revalidates every
unaddressed terminal payload on every Renew. Operators must protect stored data
from out-of-band mutation. Complete integrity audits are separate from the hot
path; history summaries are not a cryptographic proof of all payloads.

An incomplete Redis publication remains unavailable. Stop affected writers and
stop or drain their active business work before recovery, then
investigate complete persisted records/indexes and uncertain business outcomes;
do not remove only the marker, recreate an empty resource, or blindly replay the
failed mutation. Backend durability and application lease/fencing requirements
remain as documented by each driver.

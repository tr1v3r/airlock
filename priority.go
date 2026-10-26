package airlock

import "github.com/tr1v3r/airlock/internal/scheduler"

// PriorityGroup reserves waiting space and selection opportunities for a group
// of tenants. It does not reserve execution capacity or preempt running work.
// The scheduling core owns the definition; this alias preserves the local
// admission configuration surface.
type PriorityGroup = scheduler.PriorityGroup

// PriorityPolicy maps trusted tenant IDs to fixed priority groups. New snapshots
// the configuration; later changes to these maps or slices have no effect.
// Callers must not mutate the policy concurrently with New.
type PriorityPolicy = scheduler.PriorityPolicy

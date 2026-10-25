package scheduler

import "container/list"

// PriorityGroup reserves waiting space and selection opportunities for a group
// of tenants. It does not reserve execution capacity or preempt running work.
type PriorityGroup struct {
	// Name is nonempty and unique within the policy.
	Name string
	// Turns is the positive number of request selections per scheduling cycle.
	// It does not measure tokens or execution time.
	Turns int
	// MaxQueued is this group's nonnegative, non-borrowable waiting limit.
	// Zero disables waiting for this group, but still allows immediate grants.
	MaxQueued int
}

// PriorityPolicy maps trusted tenant IDs to fixed priority groups. New snapshots
// the configuration; later changes to these maps or slices have no effect.
// Callers must not mutate the policy concurrently with New.
type PriorityPolicy struct {
	// Groups lists groups from high to low priority, with nonincreasing Turns.
	// The sum of Turns must not exceed 1024. The sum of MaxQueued must not
	// exceed Config.MaxQueued; unused queue space is not shared or borrowed.
	Groups []PriorityGroup
	// DefaultGroup must name a group. It serves the default tenant and all
	// explicit tenant IDs not present in TenantGroups.
	DefaultGroup string
	// TenantGroups maps nonempty tenant IDs to existing group names.
	// The caller authenticates tenant identity; there is no request-level
	// priority override or dynamic reassignment.
	TenantGroups map[string]string
}

// PriorityQueue holds a group's bounded waiting population and tenant rotation.
type PriorityQueue struct {
	// limit is the group's fixed, non-borrowable pending-request budget.
	Limit int
	// queued includes all pending requests in the group, including a protected head.
	Queued int
	// rotation holds nonempty tenant queues in next-selection order.
	Rotation list.List
}

// PriorityState owns a compiled snapshot, retaining no caller-owned maps/slices.
type PriorityState struct {
	// fallback serves default and unmapped tenants.
	Fallback *PriorityQueue
	// tenants is the immutable explicit tenant-to-group assignment.
	Tenants map[string]*PriorityQueue
	// schedule repeats group pointers by their selection shares.
	Schedule []*PriorityQueue
	// cursor identifies the next weighted schedule slot to examine.
	Cursor int
}

// newPriorityState validates and copies a policy into independent scheduling state.
func newPriorityState(policy *PriorityPolicy, maxQueued int) (PriorityState, error) {
	if policy == nil {
		q := &PriorityQueue{Limit: maxQueued}
		return PriorityState{Fallback: q, Schedule: []*PriorityQueue{q}}, nil
	}
	const maxTurns = 1024
	if len(policy.Groups) == 0 || len(policy.Groups) > maxTurns {
		return PriorityState{}, ErrInvalidConfig
	}
	state := PriorityState{Tenants: make(map[string]*PriorityQueue, len(policy.TenantGroups))}
	groups := make(map[string]*PriorityQueue, len(policy.Groups))
	turns, queued := 0, 0
	for i, spec := range policy.Groups {
		if spec.Name == "" || groups[spec.Name] != nil || spec.Turns <= 0 || spec.Turns > maxTurns-turns ||
			(i > 0 && spec.Turns > policy.Groups[i-1].Turns) ||
			spec.MaxQueued < 0 || spec.MaxQueued > maxQueued-queued {
			return PriorityState{}, ErrInvalidConfig
		}
		turns += spec.Turns
		queued += spec.MaxQueued
		q := &PriorityQueue{Limit: spec.MaxQueued}
		groups[spec.Name] = q
		for j := 0; j < spec.Turns; j++ {
			state.Schedule = append(state.Schedule, q)
		}
	}
	state.Fallback = groups[policy.DefaultGroup]
	if state.Fallback == nil {
		return PriorityState{}, ErrInvalidConfig
	}
	for tenant, name := range policy.TenantGroups {
		q := groups[name]
		if tenant == "" || q == nil {
			return PriorityState{}, ErrInvalidConfig
		}
		state.Tenants[tenant] = q
	}
	return state, nil
}

// Group resolves a tenant to its assigned group or the fallback group.
func (p *PriorityState) Group(tenant string) *PriorityQueue {
	if q := p.Tenants[tenant]; q != nil {
		return q
	}
	return p.Fallback
}

// next requires mu and at least one queued request. That invariant bounds the
// scan to one cycle (at most 1024 slots). Skipped empty groups accumulate no credit.
func (p *PriorityState) next() *PriorityQueue {
	for {
		q := p.Schedule[p.Cursor]
		p.Cursor = (p.Cursor + 1) % len(p.Schedule)
		if q.Queued > 0 {
			return q
		}
	}
}

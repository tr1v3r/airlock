package airlock

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

type priorityQueue struct {
	limit    int
	queued   int
	rotation list.List
}

// priorityState owns a compiled snapshot, retaining no caller-owned maps/slices.
type priorityState struct {
	fallback *priorityQueue
	tenants  map[string]*priorityQueue
	schedule []*priorityQueue
	cursor   int
}

func newPriorityState(policy *PriorityPolicy, maxQueued int) (priorityState, error) {
	if policy == nil {
		q := &priorityQueue{limit: maxQueued}
		return priorityState{fallback: q, schedule: []*priorityQueue{q}}, nil
	}
	const maxTurns = 1024
	if len(policy.Groups) == 0 || len(policy.Groups) > maxTurns {
		return priorityState{}, ErrInvalidConfig
	}
	state := priorityState{tenants: make(map[string]*priorityQueue, len(policy.TenantGroups))}
	groups := make(map[string]*priorityQueue, len(policy.Groups))
	turns, queued := 0, 0
	for i, spec := range policy.Groups {
		if spec.Name == "" || groups[spec.Name] != nil || spec.Turns <= 0 || spec.Turns > maxTurns-turns ||
			(i > 0 && spec.Turns > policy.Groups[i-1].Turns) ||
			spec.MaxQueued < 0 || spec.MaxQueued > maxQueued-queued {
			return priorityState{}, ErrInvalidConfig
		}
		turns += spec.Turns
		queued += spec.MaxQueued
		q := &priorityQueue{limit: spec.MaxQueued}
		groups[spec.Name] = q
		for j := 0; j < spec.Turns; j++ {
			state.schedule = append(state.schedule, q)
		}
	}
	state.fallback = groups[policy.DefaultGroup]
	if state.fallback == nil {
		return priorityState{}, ErrInvalidConfig
	}
	for tenant, name := range policy.TenantGroups {
		q := groups[name]
		if tenant == "" || q == nil {
			return priorityState{}, ErrInvalidConfig
		}
		state.tenants[tenant] = q
	}
	return state, nil
}

func (p *priorityState) group(tenant string) *priorityQueue {
	if q := p.tenants[tenant]; q != nil {
		return q
	}
	return p.fallback
}

// next requires mu and at least one queued request. That invariant bounds the
// scan to one cycle (at most 1024 slots). Skipped empty groups accumulate no credit.
func (p *priorityState) next() *priorityQueue {
	for {
		q := p.schedule[p.cursor]
		p.cursor = (p.cursor + 1) % len(p.schedule)
		if q.queued > 0 {
			return q
		}
	}
}

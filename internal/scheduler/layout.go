package scheduler

// Layout is the structural scheduling position of a Scheduler: everything a
// persistence adapter needs to restore exact queue and rotation state. It is
// a snapshot for the module's internal state machine; tenant keys stand in
// for queue identities and waiters link records to their queue positions.
type Layout struct {
	// Rotations lists, per distinct group in weighted-schedule order, the
	// nonempty tenant keys in next-selection order.
	Rotations [][]string
	// GroupQueued lists each distinct group's pending-request budget usage,
	// including a protected head, in the same order as Rotations.
	GroupQueued []int
	// Queues lists each waiting tenant's pending waiters in FIFO order.
	Queues map[string][]*Waiter
	// Protected is the selected head waiter that cannot be bypassed, or nil.
	Protected *Waiter
	// Cursor is the next weighted schedule position to examine.
	Cursor int
}

// Layout captures the current scheduling position for a persistence adapter.
// The caller serializes access; the waiters remain owned by the Scheduler.
func (s *Scheduler) Layout() Layout {
	queues := make(map[string][]*Waiter, len(s.tenants))
	rotations := make([][]string, 0, len(s.priority.order))
	groupQueued := make([]int, 0, len(s.priority.order))
	for _, group := range s.priority.order {
		tenants := []string{}
		for e := group.rotation.Front(); e != nil; e = e.Next() {
			tq := e.Value.(*tenantQueue)
			tenants = append(tenants, tq.id)
			for n := tq.requests.Front(); n != nil; n = n.Next() {
				queues[tq.id] = append(queues[tq.id], n.Value.(*Waiter))
			}
		}
		rotations = append(rotations, tenants)
		groupQueued = append(groupQueued, group.queued)
	}
	return Layout{Rotations: rotations, GroupQueued: groupQueued, Queues: queues, Protected: s.protected, Cursor: s.priority.cursor}
}

// ScheduleSlots returns the number of weighted selection slots (the sum of
// group turns). Cursor values must be nonnegative and below it.
func (s *Scheduler) ScheduleSlots() int { return len(s.priority.schedule) }

// GroupCount returns the number of distinct priority groups in weighted-
// schedule order. Rotation lists must have exactly this length when nonempty.
func (s *Scheduler) GroupCount() int { return len(s.priority.order) }

// GroupIndex returns the position of tenant's group among the distinct groups
// in weighted-schedule order. Every tenant resolves to a group, so the result
// is always valid.
func (s *Scheduler) GroupIndex(tenant string) int { return s.priority.index[s.priority.group(tenant)] }

// HasQueue reports whether tenant currently has a nonempty waiting queue.
func (s *Scheduler) HasQueue(tenant string) bool { return s.tenants[tenant] != nil }

// SetCursor positions the weighted selection cursor. It reports false when
// cursor is outside [0, ScheduleSlots) and changes nothing.
func (s *Scheduler) SetCursor(cursor int) bool {
	if cursor < 0 || cursor >= len(s.priority.schedule) {
		return false
	}
	s.priority.cursor = cursor
	return true
}

// SetProtected selects w as the protected head. It reports false when w is
// nil, not the head of its own tenant queue, or not queued in this Scheduler,
// and changes nothing.
func (s *Scheduler) SetProtected(w *Waiter) bool {
	if w == nil || w.tenant == nil || s.tenants[w.tenant.id] != w.tenant || w.tenant.requests.Front() == nil || w.tenant.requests.Front().Value != w {
		return false
	}
	s.protected = w
	return true
}

// Package scheduler implements the admission scheduling core shared by the
// process-local Airlock and the protocol package's reference state machine:
// capacity accounting, bounded waiting queues, per-tenant FIFO order,
// round-robin tenant rotation, weighted priority-group selection and the
// protected head that cannot be bypassed while awaiting capacity.
//
// A Scheduler is a single-threaded state machine. Callers own serialization
// (the root package holds a mutex; the reference state machine runs inside a
// driver's atomic boundary) and must not copy a Scheduler. The package is
// internal: its exported surface exists for the airlock and protocol packages
// inside this module and is not a public API. Queue internals and waiters are
// never exposed outside this module.
package scheduler

import (
	"container/list"
	"errors"
)

// Shared admission errors. The identities are shared with the airlock and
// protocol packages, and the strings double as persisted rejection sentinels.
var (
	// ErrInvalidConfig reports invalid limits or a priority policy.
	ErrInvalidConfig = errors.New("airlock: invalid configuration")
	// ErrInvalidWeight reports a nonpositive requested token count.
	ErrInvalidWeight = errors.New("airlock: invalid weight")
	// ErrWeightExceedsCapacity reports a request larger than the current capacity.
	ErrWeightExceedsCapacity = errors.New("airlock: weight exceeds capacity")
	// ErrQueueFull reports that the global waiting queue is full.
	ErrQueueFull = errors.New("airlock: queue full")
	// ErrGroupQueueFull reports that the tenant's priority group waiting queue is full.
	ErrGroupQueueFull = errors.New("airlock: priority group queue full")
	// ErrTenantQueueFull reports that the tenant's waiting queue is full.
	ErrTenantQueueFull = errors.New("airlock: tenant queue full")
)

// Config defines initial capacity and fixed queue limits for admission
// scheduling. Queue limits count pending requests, not granted permits. Zero
// queue limits disable waiting.
type Config struct {
	// Capacity is the positive initial token limit; Resize can change it.
	Capacity int
	// MaxQueued is the nonnegative maximum number of pending requests.
	MaxQueued int
	// MaxQueuedPerTenant is nonnegative and must not exceed MaxQueued.
	// It does not limit a tenant's number of held tokens or permits.
	MaxQueuedPerTenant int
	// Priority optionally enables fixed weighted selection between groups of
	// tenants. Nil preserves a single group with tenant round-robin scheduling.
	Priority *PriorityPolicy
}

// Stats is a consistent snapshot, not a completion barrier. Capacity and InUse
// count tokens; ActivePermits counts granted, unreleased permits, and Queued
// counts pending requests, including a protected head request.
type Stats struct {
	// Capacity is the current token limit; InUse may exceed it after shrinking.
	Capacity int
	// InUse counts held tokens, including all tokens in each weighted permit.
	InUse int
	// ActivePermits counts grants not yet released, regardless of their weights.
	ActivePermits int
	// Queued counts pending requests, including a protected head waiting for its full weight.
	Queued int
	// WaitingTenants counts distinct tenants with pending requests, including the default tenant.
	WaitingTenants int
}

// tenantQueue connects one nonempty tenant FIFO to its group rotation.
type tenantQueue struct {
	// id is the exact tenant key, empty only for the default tenant.
	id string
	// group owns this tenant's waiting budget and rotation.
	group *priorityQueue
	// turn is this queue's node in the group rotation.
	turn *list.Element
	// requests stores pending waiters in FIFO order, including any protected head.
	requests list.List
}

// Waiter tracks one pending atomic allocation. It belongs to exactly one
// Scheduler and must only be used while that Scheduler's caller serializes
// access. Local admission arms the notification channel before dispatch; the
// reference state machine leaves it nil. Ready closes on a full grant or a
// resize rejection; cancellation removes the waiter without closing it.
type Waiter struct {
	// ready is set only by local admission and closes after a grant or resize rejection.
	// Shared scheduling leaves it nil; cancellation does not close it.
	ready chan struct{}
	// granted records the winning grant transition under the caller's serialization.
	granted bool
	// err records a terminal resize rejection, published by closing ready.
	err error
	// weight is the positive token count requested atomically.
	weight int
	// node links the waiter into its tenant's FIFO.
	node *list.Element
	// tenant owns the waiter's queue and group accounting.
	tenant *tenantQueue
}

// Ready closes after the whole request is granted or rejected by a resize.
// It never closes on removal, and it is nil until the local caller arms it.
func (w *Waiter) Ready() <-chan struct{} { return w.ready }

// Granted reports whether dispatch converted this waiter into a full grant.
func (w *Waiter) Granted() bool { return w.granted }

// Err reports a terminal resize rejection published with the ready channel.
func (w *Waiter) Err() error { return w.err }

// Weight returns the atomic token count the waiter is waiting for.
func (w *Waiter) Weight() int { return w.weight }

// Arm installs the grant notification channel. The local caller arms each
// waiter after enqueueing and before dispatch, under its own serialization;
// a second Arm call would orphan the first channel and must not happen.
func (w *Waiter) Arm() { w.ready = make(chan struct{}) }

// Scheduler controls weighted admission with FIFO waiting within each tenant
// and round-robin selection across nonempty tenant queues within each priority
// group. Optional priority groups receive weighted selection opportunities.
// New waiting tenants join the end of their group's rotation.
//
// A selected request waits for all its tokens before any later request can be
// granted. This prevents starvation of large requests, but can leave capacity
// idle. Progress requires holders to eventually release their permits.
// Fairness applies to request selection, not token counts, execution time or
// requests rejected before queueing. One tenant may hold every token when
// others are idle. Use New; the zero value is not usable, and a Scheduler must
// not be copied. Callers serialize every method call.
type Scheduler struct {
	// cfg holds immutable limits; Priority is compiled into priority rather than retained.
	cfg Config
	// used counts held tokens; queued counts pending requests across all tenants.
	used, queued int
	// active counts granted allocations awaiting release.
	active int
	// protected is the selected head that no later request may bypass.
	protected *Waiter
	// tenants indexes nonempty waiting queues; empty keys represent the default tenant.
	tenants map[string]*tenantQueue
	// priority owns group budgets and the weighted selection cursor.
	priority priorityState
}

// New constructs a Scheduler, or returns ErrInvalidConfig for invalid limits
// or priority policy. It snapshots the policy. It has no unlimited mode;
// limits, tenant assignments and selection shares cannot be changed after
// creation.
func New(cfg Config) (*Scheduler, error) {
	if cfg.Capacity <= 0 || cfg.MaxQueued < 0 || cfg.MaxQueuedPerTenant < 0 || cfg.MaxQueuedPerTenant > cfg.MaxQueued {
		return nil, ErrInvalidConfig
	}
	priority, err := newPriorityState(cfg.Priority, cfg.MaxQueued)
	if err != nil {
		return nil, err
	}
	// Retain only the compiled immutable policy, not caller-owned collections.
	cfg.Priority = nil
	return &Scheduler{cfg: cfg, tenants: make(map[string]*tenantQueue), priority: priority}, nil
}

// Capacity returns the fixed token limit configured at construction.
func (s *Scheduler) Capacity() int { return s.cfg.Capacity }

// Stats returns a consistent snapshot, not a completion barrier.
func (s *Scheduler) Stats() Stats {
	return Stats{Capacity: s.cfg.Capacity, InUse: s.used, ActivePermits: s.active, Queued: s.queued, WaitingTenants: len(s.tenants)}
}

// TryAcquire grants weight tokens immediately when no request is queued and
// capacity is available, accounting the grant and reporting success. It never
// queues, bypasses waiters, or holds a partial allocation; on failure it
// changes no state.
func (s *Scheduler) TryAcquire(weight int) bool {
	if s.queued != 0 || weight > s.cfg.Capacity-s.used {
		return false
	}
	s.used += weight
	s.active++
	return true
}

// Reserve accounts a granted allocation of weight tokens. The caller must
// have validated weight against capacity (restored grants, or an immediate
// grant checked through TryAcquire). Release returns the tokens.
func (s *Scheduler) Reserve(weight int) {
	s.used += weight
	s.active++
}

// Release returns weight tokens held by a granted allocation. The caller
// serializes access and supplies the same weight that was reserved.
func (s *Scheduler) Release(weight int) {
	s.used -= weight
	s.active--
}

// Enqueue admits one pending request of weight tokens for tenant without
// granting it. Empty tenant names select the shared default tenant. Queue
// limits are checked globally, then per group, then per tenant; rejection
// returns one of ErrQueueFull, ErrGroupQueueFull or ErrTenantQueueFull and
// changes no state. The caller serializes access and handles immediate
// admission first.
func (s *Scheduler) Enqueue(tenant string, weight int) (*Waiter, error) {
	if s.queued >= s.cfg.MaxQueued {
		return nil, ErrQueueFull
	}
	group := s.priority.group(tenant)
	if group.queued >= group.limit {
		return nil, ErrGroupQueueFull
	}
	tq := s.tenants[tenant]
	if s.cfg.MaxQueuedPerTenant == 0 || (tq != nil && tq.requests.Len() >= s.cfg.MaxQueuedPerTenant) {
		return nil, ErrTenantQueueFull
	}
	if tq == nil {
		tq = &tenantQueue{id: tenant, group: group}
		tq.turn = group.rotation.PushBack(tq)
		s.tenants[tenant] = tq
	}
	w := &Waiter{tenant: tq, weight: weight}
	w.node = tq.requests.PushBack(w)
	s.queued++
	group.queued++
	return w, nil
}

// Remove cancels one queued waiter without granting it. Empty tenant indexes
// are reclaimed immediately. The caller serializes access.
func (s *Scheduler) Remove(w *Waiter) {
	if s.protected == w {
		s.protected = nil
	}
	tq := w.tenant
	tq.requests.Remove(w.node)
	s.queued--
	tq.group.queued--
	if tq.requests.Len() == 0 {
		tq.group.rotation.Remove(tq.turn)
		delete(s.tenants, tq.id)
	}
}

// Dispatch grants whole requests in selection order until capacity blocks the
// protected head. Granted waiters are removed, accounted and marked Granted;
// their Ready channels close. The caller must serialize access; a selected
// turn is consumed even if the request is later canceled.
func (s *Scheduler) Dispatch() {
	for s.used < s.cfg.Capacity && s.queued > 0 {
		if s.protected == nil {
			group := s.priority.next()
			tq := group.rotation.Front().Value.(*tenantQueue)
			s.protected = tq.requests.Front().Value.(*Waiter)
			// Selection consumes this tenant's turn, even if it later cancels.
			// Keep its queue indexed until the selected request is removed.
			group.rotation.MoveToBack(tq.turn)
		}
		w := s.protected
		if w.weight > s.cfg.Capacity-s.used {
			return
		}
		s.Remove(w)
		s.used += w.weight
		s.active++
		w.granted = true
		if w.ready != nil {
			close(w.ready)
		}
	}
}

// Resize atomically changes the local token limit and dispatches waiting
// requests using the existing fairness policy. Nonpositive capacity returns
// ErrInvalidConfig without changing state. Setting the current capacity is a
// no-op.
//
// Shrinking never revokes grants: used may exceed capacity until holders
// release. Queued requests heavier than the new capacity are removed and
// rejected with ErrWeightExceedsCapacity through their armed notification
// channel; remaining requests retain their order and any surviving protected
// head. Cancellation, rejection and grant are arbitrated by the caller's
// serialization: whichever removes a waiter first determines its result.
// Queue limits and priority policy are unchanged. Local admission owns
// Resize; shared admission capacity stays immutable.
func (s *Scheduler) Resize(capacity int) error {
	if capacity <= 0 {
		return ErrInvalidConfig
	}
	if capacity == s.cfg.Capacity {
		return nil
	}
	previous := s.cfg.Capacity
	s.cfg.Capacity = capacity
	if capacity < previous {
		for _, tenant := range s.tenants {
			for node := tenant.requests.Front(); node != nil; {
				next := node.Next()
				w := node.Value.(*Waiter)
				if w.weight > capacity {
					s.Remove(w)
					w.err = ErrWeightExceedsCapacity
					if w.ready != nil {
						close(w.ready)
					}
				}
				node = next
			}
		}
	}
	s.Dispatch()
	return nil
}

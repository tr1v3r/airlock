package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"
)

// This oracle deliberately uses slices and request IDs, not the production
// linked lists, priorityState, enqueue, remove, dispatch, or Transition helpers.
// Each public operation is settled before the next event. Concurrent races are
// exercised separately by TestLocalConcurrentStorm.
type localModelRequest struct {
	Tenant string
	Weight int
	state  string
	err    error
}

type localModel struct {
	Cfg        Config
	Requests   []localModelRequest
	queues     map[string][]int
	Rotation   [][]string
	budgets    []int
	slots      []int
	assignment map[string]int
	Fallback   int
	Cursor     int
	Protected  int
	Used       int
}

func newLocalModel(cfg Config) *localModel {
	m := &localModel{Cfg: cfg, queues: make(map[string][]int), assignment: make(map[string]int), Protected: -1}
	if cfg.Priority == nil {
		m.Rotation = make([][]string, 1)
		m.budgets = []int{cfg.MaxQueued}
		m.slots = []int{0}
		return m
	}
	groups := make(map[string]int)
	for i, group := range cfg.Priority.Groups {
		groups[group.Name] = i
		m.Rotation = append(m.Rotation, nil)
		m.budgets = append(m.budgets, group.MaxQueued)
		for range group.Turns {
			m.slots = append(m.slots, i)
		}
	}
	m.Fallback = groups[cfg.Priority.DefaultGroup]
	for tenant, group := range cfg.Priority.TenantGroups {
		m.assignment[tenant] = groups[group]
	}
	return m
}

func (m *localModel) Group(tenant string) int {
	if group, ok := m.assignment[tenant]; ok {
		return group
	}
	return m.Fallback
}

func (m *localModel) waiting(group int) int {
	n := 0
	for tenant, queue := range m.queues {
		if group < 0 || m.Group(tenant) == group {
			n += len(queue)
		}
	}
	return n
}

func (m *localModel) arrive(tenant string, weight int, try bool) int {
	id := len(m.Requests)
	r := localModelRequest{Tenant: tenant, Weight: weight, state: "pending"}
	group := m.Group(tenant)
	switch {
	case weight <= 0:
		r.err = ErrInvalidWeight
	case weight > m.Cfg.Capacity:
		r.err = ErrWeightExceedsCapacity
	case m.waiting(-1) == 0 && weight <= m.Cfg.Capacity-m.Used:
		r.state = "granted"
		m.Used += weight
	case try:
		r.err = ErrWouldBlock
	case m.waiting(-1) >= m.Cfg.MaxQueued:
		r.err = ErrQueueFull
	case m.waiting(group) >= m.budgets[group]:
		r.err = ErrGroupQueueFull
	case len(m.queues[tenant]) >= m.Cfg.MaxQueuedPerTenant:
		r.err = ErrTenantQueueFull
	}
	if r.err != nil {
		r.state = "rejected"
	}
	m.Requests = append(m.Requests, r)
	if r.state == "pending" {
		if len(m.queues[tenant]) == 0 {
			m.Rotation[group] = append(m.Rotation[group], tenant)
		}
		m.queues[tenant] = append(m.queues[tenant], id)
		m.Schedule()
	}
	return id
}

func (m *localModel) unlink(id int) {
	tenant := m.Requests[id].Tenant
	q := m.queues[tenant]
	for i, candidate := range q {
		if candidate == id {
			q = append(q[:i], q[i+1:]...)
			break
		}
	}
	if len(q) != 0 {
		m.queues[tenant] = q
	} else {
		delete(m.queues, tenant)
		group := m.Group(tenant)
		for i, candidate := range m.Rotation[group] {
			if candidate == tenant {
				m.Rotation[group] = append(m.Rotation[group][:i], m.Rotation[group][i+1:]...)
				break
			}
		}
	}
	if m.Protected == id {
		m.Protected = -1
	}
}

func (m *localModel) Schedule() {
	for m.Used != m.Cfg.Capacity && len(m.queues) != 0 {
		if m.Protected < 0 {
			// Select by walking the finite declarative slot sequence. Empty
			// groups earn no credit, and selection consumes both turns now.
			for len(m.Rotation[m.slots[m.Cursor]]) == 0 {
				m.Cursor = (m.Cursor + 1) % len(m.slots)
			}
			group := m.slots[m.Cursor]
			m.Cursor = (m.Cursor + 1) % len(m.slots)
			tenant := m.Rotation[group][0]
			m.Rotation[group] = append(m.Rotation[group][1:], tenant)
			m.Protected = m.queues[tenant][0]
		}
		id := m.Protected
		if m.Requests[id].Weight > m.Cfg.Capacity-m.Used {
			return
		}
		m.unlink(id)
		m.Requests[id].state = "granted"
		m.Used += m.Requests[id].Weight
	}
}

func (m *localModel) cancel(id int) {
	if m.Requests[id].state == "pending" {
		m.unlink(id)
		m.Requests[id].state = "cancelled"
		m.Requests[id].err = context.Canceled
		m.Schedule()
	}
}

func (m *localModel) release(id int) {
	if m.Requests[id].state == "granted" {
		m.Requests[id].state = "released"
		m.Used -= m.Requests[id].Weight
		m.Schedule()
	}
}

type localModelSnapshot struct {
	Stats      Stats
	Cursor     int
	Rotation   [][]string
	Weights    map[string][]int
	GroupQueue []int
	Protected  string
}

func (m *localModel) snapshot() localModelSnapshot {
	s := localModelSnapshot{
		Stats:  Stats{Capacity: m.Cfg.Capacity, InUse: m.Used, Queued: m.waiting(-1), WaitingTenants: len(m.queues)},
		Cursor: m.Cursor, Rotation: make([][]string, len(m.Rotation)),
		Weights: make(map[string][]int), GroupQueue: make([]int, len(m.Rotation)),
	}
	for _, r := range m.Requests {
		if r.state == "granted" {
			s.Stats.ActivePermits++
		}
	}
	for group, rotation := range m.Rotation {
		s.Rotation[group] = append([]string(nil), rotation...)
		s.GroupQueue[group] = m.waiting(group)
	}
	for tenant, ids := range m.queues {
		for _, id := range ids {
			s.Weights[tenant] = append(s.Weights[tenant], m.Requests[id].Weight)
		}
	}
	if m.Protected >= 0 {
		r := m.Requests[m.Protected]
		s.Protected = fmt.Sprintf("%q/%d", r.Tenant, r.Weight)
	}
	return s
}

func localActualSnapshot(a *Airlock, cfg Config) localModelSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := localModelSnapshot{
		Stats:  Stats{Capacity: a.Cfg.Capacity, InUse: a.Used, ActivePermits: a.Active, Queued: a.Queued, WaitingTenants: len(a.Tenants)},
		Cursor: a.Priority.Cursor, Weights: make(map[string][]int),
	}
	groups := []*PriorityQueue{a.Priority.Fallback}
	if cfg.Priority != nil {
		groups = nil
		slot := 0
		for _, spec := range cfg.Priority.Groups {
			groups = append(groups, a.Priority.Schedule[slot])
			slot += spec.Turns
		}
	}
	for _, group := range groups {
		var tenants []string
		for node := group.Rotation.Front(); node != nil; node = node.Next() {
			tenants = append(tenants, node.Value.(*TenantQueue).ID)
		}
		s.Rotation = append(s.Rotation, tenants)
		s.GroupQueue = append(s.GroupQueue, group.Queued)
	}
	for tenant, q := range a.Tenants {
		for node := q.Requests.Front(); node != nil; node = node.Next() {
			s.Weights[tenant] = append(s.Weights[tenant], node.Value.(*Waiter).Weight)
		}
	}
	if w := a.Protected; w != nil {
		s.Protected = fmt.Sprintf("%q/%d", w.Tenant.ID, w.Weight)
	}
	return s
}

func localModelConfig(priority bool) Config {
	cfg := Config{Capacity: 7, MaxQueued: 12, MaxQueuedPerTenant: 3}
	if priority {
		cfg.Priority = &PriorityPolicy{
			Groups:       []PriorityGroup{{Name: "high", Turns: 3, MaxQueued: 6}, {Name: "medium", Turns: 2, MaxQueued: 4}, {Name: "low", Turns: 1, MaxQueued: 2}},
			DefaultGroup: "low", TenantGroups: map[string]string{"A": "high", "B": "high", "M": "medium"},
		}
	}
	return cfg
}

type localCallResult struct {
	permit *Permit
	err    error
}

type localModelCall struct {
	cancel   context.CancelFunc
	result   chan localCallResult
	permit   *Permit
	received bool
}

type localModelRun struct {
	t       testing.TB
	a       *Airlock
	m       *localModel
	ctx     context.Context
	calls   []*localModelCall
	history []string
}

func newLocalModelRun(t testing.TB, cfg Config) *localModelRun {
	t.Helper()
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	r := &localModelRun{t: t, a: a, m: newLocalModel(cfg), ctx: ctx}
	t.Cleanup(func() {
		cancel()
		for _, call := range r.calls {
			call.cancel()
			if call.permit != nil {
				call.permit.Release()
			}
		}
		// Drain successful cancellation/grant races even on an assertion failure.
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		for _, call := range r.calls {
			if !call.received {
				select {
				case result := <-call.result:
					result.permit.Release()
				case <-timer.C:
					t.Error("local model cleanup did not settle all callers")
					return
				}
			}
		}
	})
	return r
}

func (r *localModelRun) fail(format string, args ...any) {
	r.t.Helper()
	r.t.Fatalf("%s\nevents=%v", fmt.Sprintf(format, args...), r.history)
}

func (r *localModelRun) arrive(tenant string, weight int, try bool) {
	r.t.Helper()
	id := r.m.arrive(tenant, weight, try)
	r.history = append(r.history, fmt.Sprintf("arrive(%d,%q,%d,try=%v)", id, tenant, weight, try))
	ctx, cancel := context.WithCancel(r.ctx)
	call := &localModelCall{cancel: cancel, result: make(chan localCallResult, 1)}
	r.calls = append(r.calls, call)
	go func() {
		var p *Permit
		var err error
		if try {
			if tenant == "" {
				p, err = r.a.TryAcquireN(weight)
			} else {
				p, err = r.a.TryAcquireNFor(tenant, weight)
			}
		} else if tenant == "" {
			p, err = r.a.AcquireN(ctx, weight)
		} else {
			p, err = r.a.AcquireNFor(ctx, tenant, weight)
		}
		call.result <- localCallResult{p, err}
	}()
	r.settle()
}

func (r *localModelRun) cancel(id int) {
	r.history = append(r.history, fmt.Sprintf("cancel(%d)", id))
	r.m.cancel(id)
	r.calls[id].cancel()
	r.settle()
}

func (r *localModelRun) release(id int) {
	r.history = append(r.history, fmt.Sprintf("release(%d)", id))
	r.m.release(id)
	r.calls[id].permit.Release()
	r.settle()
}

func (r *localModelRun) settle() {
	r.t.Helper()
	for id, want := range r.m.Requests {
		call := r.calls[id]
		if want.state == "pending" || call.received {
			continue
		}
		select {
		case result := <-call.result:
			call.received, call.permit = true, result.permit
			if !errors.Is(result.err, want.err) || (result.permit == nil) != (want.err != nil) {
				r.fail("request=%d state=%s: permit=%v err=%v wantErr=%v", id, want.state, result.permit != nil, result.err, want.err)
			}
		case <-r.ctx.Done():
			r.fail("request=%d did not reach expected state=%s", id, want.state)
		}
	}
	// An arrival expected to remain pending has no return value as a barrier.
	// Observe queue count under the implementation mutex, not goroutine order.
	want := r.m.snapshot()
	for {
		got := localActualSnapshot(r.a, r.m.Cfg)
		if got.Stats.Queued == want.Stats.Queued {
			if !reflect.DeepEqual(got, want) {
				r.fail("state mismatch\ngot  %+v\nwant %+v", got, want)
			}
			break
		}
		select {
		case <-r.ctx.Done():
			r.fail("queue did not settle\ngot  %+v\nwant %+v", got, want)
		default:
			runtime.Gosched()
		}
	}
	for id, want := range r.m.Requests {
		if want.state == "pending" {
			select {
			case got := <-r.calls[id].result:
				r.calls[id].received, r.calls[id].permit = true, got.permit
				r.fail("pending request=%d returned early: permit=%v err=%v", id, got.permit != nil, got.err)
			default:
			}
		}
	}
}

func runLocalEvents(t testing.TB, priority bool, events []byte) {
	t.Helper()
	r := newLocalModelRun(t, localModelConfig(priority))
	// Fill capacity with two permits, allowing partial release/protection later.
	r.arrive("holder", 5, false)
	r.arrive("holder", 2, true)
	tenants := []string{"", "default", "A", "B", "M", "L", "unknown"}
	for i := 0; i+2 < len(events); i += 3 {
		action, arg, value := events[i]%8, int(events[i+1]), int(events[i+2])
		switch action {
		case 0, 1, 2, 3:
			// Include zero and oversized weights in addition to every legal weight.
			r.arrive(tenants[arg%len(tenants)], value%(r.m.Cfg.Capacity+2), action == 3)
		case 4, 5:
			var candidates []int
			for id, request := range r.m.Requests {
				if request.state == "pending" || request.state == "granted" {
					candidates = append(candidates, id)
				}
			}
			if len(candidates) != 0 {
				r.cancel(candidates[arg%len(candidates)])
			}
		case 6, 7:
			var candidates []int
			for id, request := range r.m.Requests {
				if request.state == "granted" {
					candidates = append(candidates, id)
				}
			}
			if len(candidates) != 0 {
				r.release(candidates[arg%len(candidates)])
			}
		}
	}
	// Drain by release rather than cancellation: every accepted, uncancelled
	// request must eventually succeed when all preceding holders release.
	for {
		id := -1
		for candidate, request := range r.m.Requests {
			if request.state == "granted" {
				id = candidate
				break
			}
		}
		if id < 0 {
			break
		}
		r.release(id)
	}
	if got := r.a.Stats(); got.InUse != 0 || got.ActivePermits != 0 || got.Queued != 0 || got.WaitingTenants != 0 {
		r.fail("drain leaked state: %+v", got)
	}
}

func TestLocalDynamicModel(t *testing.T) {
	for _, priority := range []bool{false, true} {
		for seed := int64(0); seed < 16; seed++ {
			t.Run(fmt.Sprintf("priority=%v/seed=%d", priority, seed), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))
				events := make([]byte, 3*128)
				for i := range events {
					events[i] = byte(rng.Intn(256))
				}
				runLocalEvents(t, priority, events)
			})
		}
	}
}

func TestLocalProtectedArrivalAndCancellationModel(t *testing.T) {
	for _, priority := range []bool{false, true} {
		t.Run(fmt.Sprintf("priority=%v", priority), func(t *testing.T) {
			r := newLocalModelRun(t, localModelConfig(priority))
			r.arrive("holder", 6, false)
			r.arrive("A", 7, false) // Protected while only one token is idle.
			r.arrive("A", 2, false) // Append within the protected tenant.
			r.arrive("B", 1, false) // New tenant joins after selection.
			r.arrive("M", 1, true)  // Try must not bypass any group.
			r.arrive("L", 1, false)
			r.cancel(1) // Consume A's selected turn, not B's or the next group slot.
			r.release(0)
			for id, request := range r.m.Requests {
				if request.state == "granted" {
					r.release(id)
				}
			}
		})
	}
}

// Inject representative faults into a quiescent implementation instance, not
// into the expected snapshot. Restore each fault before any release/cancel so
// this checks the oracle's observation path without changing production code.
func TestLocalModelMutationSensitivity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Airlock) func()
	}{
		{"lost capacity", func(a *Airlock) func() { a.Used--; return func() { a.Used++ } }},
		{"duplicate permit", func(a *Airlock) func() { a.Active++; return func() { a.Active-- } }},
		{"refunded selection", func(a *Airlock) func() {
			old := a.Priority.Cursor
			a.Priority.Cursor = 0
			return func() { a.Priority.Cursor = old }
		}},
		{"lost protection", func(a *Airlock) func() {
			old := a.Protected
			a.Protected = nil
			return func() { a.Protected = old }
		}},
		{"FIFO inversion", func(a *Airlock) func() {
			queue := &a.Tenants["A"].Requests
			oldHead := queue.Front()
			queue.MoveToBack(oldHead)
			return func() { queue.MoveToFront(oldHead) }
		}},
		{"lost group entry", func(a *Airlock) func() {
			group := a.Priority.Group("A")
			group.Queued--
			return func() { group.Queued++ }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newLocalModelRun(t, localModelConfig(true))
			r.arrive("holder", 6, false)
			r.arrive("A", 7, false)
			r.arrive("A", 2, false)
			r.arrive("B", 1, false)
			want := r.m.snapshot()
			r.a.mu.Lock()
			restore := tc.mutate(r.a)
			r.a.mu.Unlock()
			changed := localActualSnapshot(r.a, r.m.Cfg)
			r.a.mu.Lock()
			restore()
			r.a.mu.Unlock()
			if reflect.DeepEqual(changed, want) {
				t.Fatal("oracle did not detect the injected implementation-state fault")
			}
			r.settle()
		})
	}
}

func TestLocalQueueRecoveryModel(t *testing.T) {
	cfg := localModelConfig(true)
	cfg.MaxQueued = 8
	cfg.Priority.Groups[0].MaxQueued = 4
	cfg.Priority.Groups[1].MaxQueued = 2
	cfg.Priority.Groups[2].MaxQueued = 2
	r := newLocalModelRun(t, cfg)
	r.arrive("holder", 7, false)
	for range 3 {
		r.arrive("A", 1, false)
	}
	r.arrive("A", 1, false) // Per-tenant rejection, without consuming a turn.
	r.arrive("B", 1, false)
	r.arrive("B", 1, false) // Group rejection while global space remains.
	for _, tenant := range []string{"M", "M", "L", "L"} {
		r.arrive(tenant, 1, false)
	}
	r.arrive("A", 1, false) // Global error must mask both group and tenant errors.
	r.cancel(8)             // Cancel M's tail and reuse that group's queue budget.
	r.arrive("M", 1, true)  // Free queue space is not immediate capacity.
	r.arrive("M", 1, false)
	// Anchor expected errors independently of the reference scheduler itself.
	for id, want := range map[int]error{4: ErrTenantQueueFull, 6: ErrGroupQueueFull, 11: ErrQueueFull, 12: ErrWouldBlock} {
		if got := r.m.Requests[id].err; !errors.Is(got, want) {
			t.Fatalf("oracle error anchor id=%d: got=%v want=%v", id, got, want)
		}
	}
	r.release(0)
	for id, request := range r.m.Requests {
		if request.state == "granted" {
			r.release(id)
		}
	}
	if got := r.a.Stats(); got != (Stats{Capacity: cfg.Capacity}) {
		t.Fatalf("queue budget recovery leaked: %+v", got)
	}
}

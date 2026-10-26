package airlock

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
// linked lists, priorityState, enqueue, remove or dispatch helpers from the
// internal scheduler. Each public operation is settled before the next event.
// Concurrent races are exercised separately by TestLocalConcurrentStorm.
type localModelRequest struct {
	tenant string
	weight int
	state  string
	err    error
}

type localModel struct {
	cfg        Config
	requests   []localModelRequest
	queues     map[string][]int
	rotation   [][]string
	budgets    []int
	slots      []int
	assignment map[string]int
	fallback   int
	cursor     int
	protected  int
	used       int
}

func newLocalModel(cfg Config) *localModel {
	m := &localModel{cfg: cfg, queues: make(map[string][]int), assignment: make(map[string]int), protected: -1}
	if cfg.Priority == nil {
		m.rotation = make([][]string, 1)
		m.budgets = []int{cfg.MaxQueued}
		m.slots = []int{0}
		return m
	}
	groups := make(map[string]int)
	for i, group := range cfg.Priority.Groups {
		groups[group.Name] = i
		m.rotation = append(m.rotation, nil)
		m.budgets = append(m.budgets, group.MaxQueued)
		for range group.Turns {
			m.slots = append(m.slots, i)
		}
	}
	m.fallback = groups[cfg.Priority.DefaultGroup]
	for tenant, group := range cfg.Priority.TenantGroups {
		m.assignment[tenant] = groups[group]
	}
	return m
}

func (m *localModel) group(tenant string) int {
	if group, ok := m.assignment[tenant]; ok {
		return group
	}
	return m.fallback
}

func (m *localModel) waiting(group int) int {
	n := 0
	for tenant, queue := range m.queues {
		if group < 0 || m.group(tenant) == group {
			n += len(queue)
		}
	}
	return n
}

func (m *localModel) arrive(tenant string, weight int, try bool) int {
	id := len(m.requests)
	r := localModelRequest{tenant: tenant, weight: weight, state: "pending"}
	group := m.group(tenant)
	switch {
	case weight <= 0:
		r.err = ErrInvalidWeight
	case weight > m.cfg.Capacity:
		r.err = ErrWeightExceedsCapacity
	case m.waiting(-1) == 0 && weight <= m.cfg.Capacity-m.used:
		r.state = "granted"
		m.used += weight
	case try:
		r.err = ErrWouldBlock
	case m.waiting(-1) >= m.cfg.MaxQueued:
		r.err = ErrQueueFull
	case m.waiting(group) >= m.budgets[group]:
		r.err = ErrGroupQueueFull
	case len(m.queues[tenant]) >= m.cfg.MaxQueuedPerTenant:
		r.err = ErrTenantQueueFull
	}
	if r.err != nil {
		r.state = "rejected"
	}
	m.requests = append(m.requests, r)
	if r.state == "pending" {
		if len(m.queues[tenant]) == 0 {
			m.rotation[group] = append(m.rotation[group], tenant)
		}
		m.queues[tenant] = append(m.queues[tenant], id)
		m.schedule()
	}
	return id
}

func (m *localModel) unlink(id int) {
	tenant := m.requests[id].tenant
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
		group := m.group(tenant)
		for i, candidate := range m.rotation[group] {
			if candidate == tenant {
				m.rotation[group] = append(m.rotation[group][:i], m.rotation[group][i+1:]...)
				break
			}
		}
	}
	if m.protected == id {
		m.protected = -1
	}
}

func (m *localModel) schedule() {
	for m.used != m.cfg.Capacity && len(m.queues) != 0 {
		if m.protected < 0 {
			// Select by walking the finite declarative slot sequence. Empty
			// groups earn no credit, and selection consumes both turns now.
			for len(m.rotation[m.slots[m.cursor]]) == 0 {
				m.cursor = (m.cursor + 1) % len(m.slots)
			}
			group := m.slots[m.cursor]
			m.cursor = (m.cursor + 1) % len(m.slots)
			tenant := m.rotation[group][0]
			m.rotation[group] = append(m.rotation[group][1:], tenant)
			m.protected = m.queues[tenant][0]
		}
		id := m.protected
		if m.requests[id].weight > m.cfg.Capacity-m.used {
			return
		}
		m.unlink(id)
		m.requests[id].state = "granted"
		m.used += m.requests[id].weight
	}
}

func (m *localModel) cancel(id int) {
	if m.requests[id].state == "pending" {
		m.unlink(id)
		m.requests[id].state = "cancelled"
		m.requests[id].err = context.Canceled
		m.schedule()
	}
}

func (m *localModel) release(id int) {
	if m.requests[id].state == "granted" {
		m.requests[id].state = "released"
		m.used -= m.requests[id].weight
		m.schedule()
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
		Stats:  Stats{Capacity: m.cfg.Capacity, InUse: m.used, Queued: m.waiting(-1), WaitingTenants: len(m.queues)},
		Cursor: m.cursor, Rotation: make([][]string, len(m.rotation)),
		Weights: make(map[string][]int), GroupQueue: make([]int, len(m.rotation)),
	}
	for _, r := range m.requests {
		if r.state == "granted" {
			s.Stats.ActivePermits++
		}
	}
	for group, rotation := range m.rotation {
		s.Rotation[group] = append([]string(nil), rotation...)
		s.GroupQueue[group] = m.waiting(group)
	}
	for tenant, ids := range m.queues {
		for _, id := range ids {
			s.Weights[tenant] = append(s.Weights[tenant], m.requests[id].weight)
		}
	}
	if m.protected >= 0 {
		r := m.requests[m.protected]
		s.Protected = fmt.Sprintf("%q/%d", r.tenant, r.weight)
	}
	return s
}

func localActualSnapshot(a *Airlock) localModelSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	layout := a.sched.Layout()
	s := localModelSnapshot{
		Stats:  a.sched.Stats(),
		Cursor: layout.Cursor, Weights: make(map[string][]int),
	}
	for i, group := range layout.Rotations {
		var tenants []string
		for _, tenant := range group {
			tenants = append(tenants, tenant)
			for _, w := range layout.Queues[tenant] {
				s.Weights[tenant] = append(s.Weights[tenant], w.Weight())
			}
		}
		s.Rotation = append(s.Rotation, tenants)
		s.GroupQueue = append(s.GroupQueue, layout.GroupQueued[i])
	}
	if w := layout.Protected; w != nil {
		for tenant, ws := range layout.Queues {
			for _, candidate := range ws {
				if candidate == w {
					s.Protected = fmt.Sprintf("%q/%d", tenant, w.Weight())
				}
			}
		}
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
	for id, want := range r.m.requests {
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
		got := localActualSnapshot(r.a)
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
	for id, want := range r.m.requests {
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
			r.arrive(tenants[arg%len(tenants)], value%(r.m.cfg.Capacity+2), action == 3)
		case 4, 5:
			var candidates []int
			for id, request := range r.m.requests {
				if request.state == "pending" || request.state == "granted" {
					candidates = append(candidates, id)
				}
			}
			if len(candidates) != 0 {
				r.cancel(candidates[arg%len(candidates)])
			}
		case 6, 7:
			var candidates []int
			for id, request := range r.m.requests {
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
		for candidate, request := range r.m.requests {
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
			for id, request := range r.m.requests {
				if request.state == "granted" {
					r.release(id)
				}
			}
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
		if got := r.m.requests[id].err; !errors.Is(got, want) {
			t.Fatalf("oracle error anchor id=%d: got=%v want=%v", id, got, want)
		}
	}
	r.release(0)
	for id, request := range r.m.requests {
		if request.state == "granted" {
			r.release(id)
		}
	}
	if got := r.a.Stats(); got != (Stats{Capacity: cfg.Capacity}) {
		t.Fatalf("queue budget recovery leaked: %+v", got)
	}
}

func FuzzLocalDynamicModel(f *testing.F) {
	// Byte zero selects priority; remaining triples are action/identity/weight.
	// The replay always starts with full capacity and finishes by releasing all
	// grants. Truncation bounds goroutines, queue memory and runtime per input.
	f.Add([]byte{0})
	f.Add([]byte{1, 0, 2, 7, 0, 2, 2, 0, 3, 1, 6, 1, 0, 4, 2, 0, 3, 4, 1, 6, 0, 0})
	f.Add([]byte{0, 0, 0, 7, 0, 1, 7, 3, 2, 1, 4, 2, 0, 6, 0, 0, 7, 0, 0})
	f.Add([]byte{1, 0, 2, 0, 0, 2, 8, 3, 3, 8, 0, 6, 3, 0, 5, 4, 5, 2, 0, 7, 1, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 {
			return
		}
		const maxBytes = 1 + 3*96
		if len(input) > maxBytes {
			input = input[:maxBytes]
		}
		runLocalEvents(t, input[0]&1 != 0, input[1:])
	})
}

package scheduler

import (
	"reflect"
	"testing"
)

// observationSnapshot mirrors the structural observation a persistence adapter
// or model oracle makes through the exported surface. It must be sensitive to
// every scheduling field, so injected internal faults are detectable.
type observationSnapshot struct {
	Stats       Stats
	Rotations   [][]string
	GroupQueued []int
	Weights     map[string][]int
	Protected   string
	Cursor      int
}

func observation(s *Scheduler) observationSnapshot {
	layout := s.Layout()
	out := observationSnapshot{Stats: s.Stats(), Rotations: layout.Rotations, GroupQueued: layout.GroupQueued, Weights: make(map[string][]int), Cursor: layout.Cursor}
	for tenant, ws := range layout.Queues {
		for _, w := range ws {
			out.Weights[tenant] = append(out.Weights[tenant], w.Weight())
			if w == layout.Protected {
				out.Protected = tenant
			}
		}
	}
	return out
}

// sensitivityFixture builds one granted holder and a mixed waiting population.
func sensitivityFixture(t *testing.T) *Scheduler {
	t.Helper()
	s, err := New(Config{Capacity: 7, MaxQueued: 12, MaxQueuedPerTenant: 3, Priority: &PriorityPolicy{
		Groups:       []PriorityGroup{{Name: "high", Turns: 3, MaxQueued: 6}, {Name: "medium", Turns: 2, MaxQueued: 4}, {Name: "low", Turns: 1, MaxQueued: 2}},
		DefaultGroup: "low", TenantGroups: map[string]string{"A": "high", "B": "high"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !s.TryAcquire(6) {
		t.Fatal("holder not granted")
	}
	for _, arrival := range []struct {
		tenant string
		weight int
	}{{"A", 7}, {"A", 2}, {"B", 1}} {
		if _, err := s.Enqueue(arrival.tenant, arrival.weight); err != nil {
			t.Fatalf("enqueue %s/%d: %v", arrival.tenant, arrival.weight, err)
		}
		s.Dispatch()
	}
	return s
}

// Inject representative faults into a quiescent implementation instance, not
// into the expected snapshot. Restore each fault before any release/cancel so
// this checks the observation path without changing production code.
func TestObservationMutationSensitivity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Scheduler) func()
	}{
		{"lost capacity", func(s *Scheduler) func() { s.used--; return func() { s.used++ } }},
		{"duplicate permit", func(s *Scheduler) func() { s.active++; return func() { s.active-- } }},
		{"refunded selection", func(s *Scheduler) func() {
			old := s.priority.cursor
			s.priority.cursor = 0
			return func() { s.priority.cursor = old }
		}},
		{"lost protection", func(s *Scheduler) func() {
			old := s.protected
			s.protected = nil
			return func() { s.protected = old }
		}},
		{"FIFO inversion", func(s *Scheduler) func() {
			queue := &s.tenants["A"].requests
			oldHead := queue.Front()
			queue.MoveToBack(oldHead)
			return func() { queue.MoveToFront(oldHead) }
		}},
		{"lost group entry", func(s *Scheduler) func() {
			group := s.priority.group("A")
			group.queued--
			return func() { group.queued++ }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := sensitivityFixture(t)
			want := observation(s)
			restore := tc.mutate(s)
			changed := observation(s)
			restore()
			if reflect.DeepEqual(changed, want) {
				t.Fatal("observation did not detect the injected implementation-state fault")
			}
		})
	}
}

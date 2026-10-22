package airlock

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type stateHarness struct {
	t    *testing.T
	now  time.Time
	data []byte
}

func newStateHarness(t *testing.T) *stateHarness {
	return &stateHarness{t: t, now: time.Unix(1000, 0).UTC()}
}
func (h *stateHarness) call(kind Operation, r Request, lease string, wantErr error) Result {
	h.t.Helper()
	data, res, err := Transition(h.data, h.now, Command{Kind: kind, Request: r, LeaseID: lease})
	if !errors.Is(err, wantErr) {
		h.t.Fatalf("%s %s: got %v want %v", kind, r.Tenant, err, wantErr)
	}
	if data != nil {
		h.data = data
	}
	return res
}
func (h *stateHarness) snapshot() durableSnapshot {
	h.t.Helper()
	var s durableSnapshot
	if err := json.Unmarshal(h.data, &s); err != nil {
		h.t.Fatal(err)
	}
	return s
}
func expectState(t *testing.T, r Result, want Status) {
	t.Helper()
	if r.Status != want {
		t.Fatalf("status %q want %q: %+v", r.Status, want, r)
	}
}

func TestTransitionLeaseLifecycle(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	r := stateTestRequest(h.now, cfg, "a", 3)
	first := h.call(Submit, r, "", nil)
	expectState(t, first, Granted)
	if first.Remaining != cfg.LeaseTTL || first.LeaseID == "" || first.Weight != 3 || first.Revision != 1 {
		t.Fatalf("initial grant: %+v", first)
	}
	h.now = h.now.Add(100 * time.Millisecond)
	again := h.call(Submit, r, "", nil)
	if again.LeaseID != first.LeaseID || again.Remaining != 900*time.Millisecond || again.Revision != 2 {
		t.Fatalf("retry refreshed grant: %+v", again)
	}
	h.call(Renew, r, "wrong", ErrLeaseLost)
	h.call(Release, r, "wrong", ErrLeaseLost)
	renewed := h.call(Renew, r, first.LeaseID, nil)
	if renewed.Remaining != cfg.LeaseTTL {
		t.Fatal(renewed)
	}
	h.now = h.now.Add(cfg.LeaseTTL - time.Millisecond)
	expectState(t, h.call(Observe, r, "", nil), Granted)
	h.now = h.now.Add(time.Millisecond)
	expectState(t, h.call(Observe, r, "", nil), Expired)
	h.call(Renew, r, first.LeaseID, ErrLeaseLost)
	expectState(t, h.call(Release, r, first.LeaseID, nil), Expired)
	next := stateTestRequest(h.now, cfg, "next", 3)
	lease := h.call(Submit, next, "", nil)
	expectState(t, lease, Granted)
	expectState(t, h.call(Release, next, lease.LeaseID, nil), Released)
	expectState(t, h.call(Release, next, lease.LeaseID, nil), Released)
	h.call(Renew, next, lease.LeaseID, ErrLeaseLost)
	third := stateTestRequest(h.now, cfg, "third", 3)
	expectState(t, h.call(Submit, third, "", nil), Granted)
}

func TestTransitionCancelBeforeSubmitAndActive(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	r := stateTestRequest(h.now, cfg, "a", 3)
	expectState(t, h.call(Cancel, r, "", nil), Cancelled)
	expectState(t, h.call(Submit, r, "", nil), Cancelled)
	expectState(t, h.call(Cancel, r, "", nil), Cancelled)
	active := stateTestRequest(h.now, cfg, "active", 3)
	lease := h.call(Submit, active, "", nil)
	queued := stateTestRequest(h.now, cfg, "queue", 2)
	expectState(t, h.call(Submit, queued, "", nil), Queued)
	expectState(t, h.call(Cancel, active, "", nil), Cancelled)
	got := h.call(Observe, queued, "", nil)
	expectState(t, got, Granted)
	h.call(Renew, active, lease.LeaseID, ErrLeaseLost)
	expectState(t, h.call(Cancel, queued, "", nil), Cancelled)
}

func TestTransitionRecoveryHistoryAndLateSubmission(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	cfg.LeaseTTL = time.Minute
	cfg.MaxRecords = 2
	a := stateTestRequest(h.now, cfg, "a", 1)
	a.WaitUntil = a.SubmitUntil
	ar := h.call(Submit, a, "", nil)
	h.call(Release, a, ar.LeaseID, nil)
	b := stateTestRequest(h.now, cfg, "b", 1)
	br := h.call(Submit, b, "", nil)
	c := stateTestRequest(h.now, cfg, "c", 1)
	h.call(Submit, c, "", ErrHistoryFull)
	h.now = h.now.Add(2 * time.Second)
	h.call(Observe, b, "", nil)
	if len(h.snapshot().Records) != 2 {
		t.Fatal("terminal record collected before submit deadline")
	}
	h.now = a.SubmitUntil
	h.call(Submit, a, "", ErrSubmitExpired)
	if len(h.snapshot().Records) != 1 {
		t.Fatal("terminal record not collected at both bounds")
	}
	// A live renewed grant survives the immutable submission deadline and retention.
	h.call(Renew, b, br.LeaseID, nil)
	expectState(t, h.call(Submit, b, "", nil), Granted)
	h.call(Release, b, br.LeaseID, nil)
	h.now = h.now.Add(cfg.Retention - time.Millisecond)
	expectState(t, h.call(Observe, b, "", nil), Released)
	h.now = h.now.Add(time.Millisecond)
	h.call(Observe, b, "", ErrSubmitExpired)
	if len(h.snapshot().Records) != 0 {
		t.Fatal("released history retained beyond retention")
	}
	// Old identity cannot be reused by extending its submission deadline.
	changed := a
	changed.SubmitUntil = changed.SubmitUntil.Add(time.Minute)
	h.call(Submit, changed, "", ErrInvalidRequest)
}

func TestTransitionExpirySweepsBeforeDispatch(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	holder := stateTestRequest(h.now, cfg, "hold", 3)
	h.call(Submit, holder, "", nil)
	expired := stateTestRequest(h.now, cfg, "expired", 3)
	expired.WaitUntil = h.now.Add(time.Second)
	live := stateTestRequest(h.now, cfg, "live", 3)
	h.call(Submit, expired, "", nil)
	h.call(Submit, live, "", nil)
	h.now = h.now.Add(time.Second)
	expectState(t, h.call(Observe, live, "", nil), Granted)
	s := h.snapshot()
	if s.Records[expired.ID].Status != Expired || s.Records[holder.ID].Status != Expired {
		t.Fatal("expiry sweep did not remove both blockers")
	}
	r := stateTestRequest(h.now, cfg, "already expired", 1)
	r.WaitUntil = h.now
	expectState(t, h.call(Submit, r, "", nil), Expired)
}

func TestTransitionErrorIdentityAndMaintenance(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	r := stateTestRequest(h.now, cfg, "a", 1)
	h.call(Observe, r, "", ErrNotFound)
	before := h.snapshot().Revision
	h.call(Renew, r, "x", ErrNotFound)
	if h.snapshot().Revision != before+1 {
		t.Fatal("business error lost maintenance revision")
	}
	future := stateTestRequest(h.now, cfg, "future", 1)
	x := NewRequest(h.now.Add(cfg.SubmitWindow + time.Millisecond))
	future.ID = x.ID
	future.SubmitUntil = x.SubmitUntil
	h.call(Submit, future, "", ErrInvalidRequest)
	h.call(Submit, r, "", nil)
	changed := r
	changed.Owner = NewRequest(h.now).Owner
	h.call(Observe, changed, "", ErrRequestConflict)
	changed = r
	changed.Weight = 2
	h.call(Submit, changed, "", ErrRequestConflict)
	changed = r
	changed.Config.Admission.Capacity = 4
	h.call(Observe, changed, "", ErrConfigConflict)
	changed = r
	changed.Resource = "other"
	h.call(Observe, changed, "", ErrConfigConflict)
	h.call(Operation("invalid"), r, "", ErrInvalidRequest)
}

func TestTransitionStableAdmissionRejections(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*ResourceConfig)
		want   error
	}{
		{"global", func(c *ResourceConfig) { c.Admission.MaxQueued = 1; c.Admission.MaxQueuedPerTenant = 1 }, ErrQueueFull},
		{"tenant", func(c *ResourceConfig) { c.Admission.MaxQueuedPerTenant = 1 }, ErrTenantQueueFull},
		{"group", func(c *ResourceConfig) {
			c.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "all", Turns: 1, MaxQueued: 1}}, DefaultGroup: "all"}
		}, ErrGroupQueueFull},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newStateHarness(t)
			cfg := stateTestConfig()
			tc.modify(&cfg)
			holder := stateTestRequest(h.now, cfg, "hold", 3)
			lease := h.call(Submit, holder, "", nil)
			waiting := stateTestRequest(h.now, cfg, "same", 1)
			h.call(Submit, waiting, "", nil)
			rejected := stateTestRequest(h.now, cfg, "same", 1)
			expectState(t, h.call(Submit, rejected, "", tc.want), Rejected)
			h.call(Release, holder, lease.LeaseID, nil)
			expectState(t, h.call(Submit, rejected, "", tc.want), Rejected)
			expectState(t, h.call(Cancel, rejected, "", tc.want), Rejected)
		})
	}
	if !errors.Is(admissionError("unknown rejection"), ErrCorruptState) {
		t.Fatal("unknown rejection not corrupt")
	}
}

func TestTransitionTenantRoundRobinAcrossSnapshots(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	cfg.Admission.Capacity = 1
	holder := stateTestRequest(h.now, cfg, "hold", 1)
	last := h.call(Submit, holder, "", nil)
	previous := holder
	var order []Request
	byTenant := map[string][]Request{}
	for _, tenant := range []string{"a", "b", "c"} {
		for i := 0; i < 3; i++ {
			r := stateTestRequest(h.now, cfg, tenant, 1)
			byTenant[tenant] = append(byTenant[tenant], r)
			h.call(Submit, r, "", nil)
		}
	}
	for i := 0; i < 3; i++ {
		for _, tenant := range []string{"a", "b", "c"} {
			order = append(order, byTenant[tenant][i])
		}
	}
	for i, r := range order {
		h.call(Release, previous, last.LeaseID, nil)
		last = h.call(Observe, r, "", nil)
		expectState(t, last, Granted)
		s := h.snapshot()
		for j := i + 1; j < len(order); j++ {
			if s.Records[order[j].ID].Status != Queued {
				t.Fatalf("request %d bypassed %d", j, i)
			}
		}
		previous = r
	}
}

func TestTransitionPriorityFullCycles(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	cfg.Admission.Capacity = 1
	cfg.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "high", Turns: 8, MaxQueued: 50}, {Name: "normal", Turns: 3, MaxQueued: 30}, {Name: "low", Turns: 1, MaxQueued: 20}}, DefaultGroup: "low", TenantGroups: map[string]string{"h": "high", "m": "normal"}}
	holder := stateTestRequest(h.now, cfg, "holder", 1)
	last := h.call(Submit, holder, "", nil)
	previous := holder
	queues := map[string][]Request{}
	for _, tenant := range []string{"l", "m", "h"} {
		count := map[string]int{"h": 24, "m": 9, "l": 3}[tenant]
		for i := 0; i < count; i++ {
			r := stateTestRequest(h.now, cfg, tenant, 1)
			queues[tenant] = append(queues[tenant], r)
			h.call(Submit, r, "", nil)
		}
	}
	for cycle := 0; cycle < 3; cycle++ {
		for _, tenant := range []string{"h", "h", "h", "h", "h", "h", "h", "h", "m", "m", "m", "l"} {
			r := queues[tenant][0]
			queues[tenant] = queues[tenant][1:]
			h.call(Release, previous, last.LeaseID, nil)
			last = h.call(Observe, r, "", nil)
			expectState(t, last, Granted)
			previous = r
		}
	}
	h.call(Release, previous, last.LeaseID, nil)
	if len(h.snapshot().Queues) != 0 {
		t.Fatal("queues not drained")
	}
}

func TestTransitionProtectedHeadSurvivesRestartAndCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			h := newStateHarness(t)
			cfg := stateTestConfig()
			cfg.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 50}, {Name: "low", Turns: 1, MaxQueued: 50}}, DefaultGroup: "low", TenantGroups: map[string]string{"high": "high"}}
			holder := stateTestRequest(h.now, cfg, "holder", 2)
			lease := h.call(Submit, holder, "", nil)
			big := stateTestRequest(h.now, cfg, "low", 3)
			expectState(t, h.call(Submit, big, "", nil), Queued)
			small := stateTestRequest(h.now, cfg, "high", 1)
			expectState(t, h.call(Submit, small, "", nil), Queued)
			s := h.snapshot()
			if s.Protected != big.ID {
				t.Fatalf("protected %q", s.Protected)
			}
			cursor := s.Cursor
			for i := 0; i < 3; i++ {
				expectState(t, h.call(Observe, small, "", nil), Queued)
				if h.snapshot().Cursor != cursor {
					t.Fatal("restore reselected protected head")
				}
			}
			if cancel {
				h.call(Cancel, big, "", nil)
				expectState(t, h.call(Observe, small, "", nil), Granted)
			} else {
				h.call(Release, holder, lease.LeaseID, nil)
				b := h.call(Observe, big, "", nil)
				expectState(t, b, Granted)
				expectState(t, h.call(Observe, small, "", nil), Queued)
				h.call(Release, big, b.LeaseID, nil)
				expectState(t, h.call(Observe, small, "", nil), Granted)
			}
		})
	}
}

func TestTransitionCorruptSnapshots(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	holder := stateTestRequest(h.now, cfg, "holder", 3)
	h.call(Submit, holder, "", nil)
	a := stateTestRequest(h.now, cfg, "a", 1)
	b := stateTestRequest(h.now, cfg, "a", 1)
	h.call(Submit, a, "", nil)
	h.call(Submit, b, "", nil)
	baseline := append([]byte(nil), h.data...)
	mutations := map[string]func(*durableSnapshot){
		"version":                func(s *durableSnapshot) { s.Version = 2 },
		"records nil":            func(s *durableSnapshot) { s.Records = nil },
		"revision overflow":      func(s *durableSnapshot) { s.Revision = ^uint64(0) },
		"backward time":          func(s *durableSnapshot) { s.Time = h.now.Add(time.Millisecond) },
		"negative cursor":        func(s *durableSnapshot) { s.Cursor = -1 },
		"large cursor":           func(s *durableSnapshot) { s.Cursor = 1 },
		"rotation count":         func(s *durableSnapshot) { s.Rotations = append(s.Rotations, []string{}) },
		"nil record":             func(s *durableSnapshot) { s.Records[b.ID] = nil },
		"id mismatch":            func(s *durableSnapshot) { s.Records[b.ID].Request.ID = a.ID },
		"resource mismatch":      func(s *durableSnapshot) { s.Records[b.ID].Request.Resource = "other" },
		"invalid request":        func(s *durableSnapshot) { s.Records[b.ID].Request.Weight = 0 },
		"unknown status":         func(s *durableSnapshot) { s.Records[b.ID].Status = "invalid" },
		"capacity excess":        func(s *durableSnapshot) { s.Records[b.ID].Request.Config = cfg; grant(s.Records[b.ID], h.now) },
		"duplicate tenant":       func(s *durableSnapshot) { s.Rotations[0] = append(s.Rotations[0], "a") },
		"empty tenant queue":     func(s *durableSnapshot) { s.Queues["a"] = nil },
		"missing queue record":   func(s *durableSnapshot) { s.Queues["a"][0] = "missing" },
		"grant in queue":         func(s *durableSnapshot) { s.Queues["a"][0] = holder.ID },
		"wrong tenant":           func(s *durableSnapshot) { s.Records[b.ID].Request.Tenant = "b" },
		"duplicate queue record": func(s *durableSnapshot) { s.Queues["a"][1] = a.ID },
		"orphan queued":          func(s *durableSnapshot) { s.Queues["a"] = s.Queues["a"][:1] },
		"extra queue":            func(s *durableSnapshot) { s.Queues["extra"] = []string{} },
		"missing protected":      func(s *durableSnapshot) { s.Protected = "missing" },
		"nonhead protected":      func(s *durableSnapshot) { s.Protected = b.ID },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			var s durableSnapshot
			if err := json.Unmarshal(baseline, &s); err != nil {
				t.Fatal(err)
			}
			mutate(&s)
			data, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			out, _, err := Transition(data, h.now, Command{Kind: Observe, Request: holder})
			if !errors.Is(err, ErrCorruptState) || out != nil {
				t.Fatalf("got data=%t err=%v", out != nil, err)
			}
		})
	}
	for _, data := range [][]byte{[]byte("{"), []byte("null")} {
		if _, _, err := Transition(data, h.now, Command{Kind: Observe, Request: holder}); !errors.Is(err, ErrCorruptState) {
			t.Fatal(err)
		}
	}
}

func TestDurableRestoreRejectsInvalidLimits(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	holder := stateTestRequest(h.now, cfg, "holder", 3)
	h.call(Submit, holder, "", nil)
	a := stateTestRequest(h.now, cfg, "a", 1)
	b := stateTestRequest(h.now, cfg, "a", 1)
	h.call(Submit, a, "", nil)
	h.call(Submit, b, "", nil)
	baseline := h.snapshot()
	for name, mutate := range map[string]func(*durableSnapshot){
		"invalid admission": func(s *durableSnapshot) { s.Config.Admission.Capacity = 0 },
		"records limit":     func(s *durableSnapshot) { s.Config.MaxRecords = 1 },
		"queue limit": func(s *durableSnapshot) {
			s.Config.Admission.MaxQueuedPerTenant = 1
			for _, rec := range s.Records {
				rec.Request.Config = s.Config
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			data, _ := json.Marshal(baseline)
			var s durableSnapshot
			if err := json.Unmarshal(data, &s); err != nil {
				t.Fatal(err)
			}
			mutate(&s)
			if _, _, err := s.restore(); !errors.Is(err, ErrCorruptState) {
				t.Fatal(err)
			}
		})
	}
}

func TestTransitionSnapshotsAreDeterministic(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	holder := stateTestRequest(h.now, cfg, "hold", 3)
	h.call(Submit, holder, "", nil)
	for i := 0; i < 6; i++ {
		r := stateTestRequest(h.now, cfg, fmt.Sprint(i%2), 1)
		h.call(Submit, r, "", nil)
	}
	original := append([]byte(nil), h.data...)
	h.now = h.now.Add(time.Second)
	data, result, err := Transition(original, h.now, Command{Kind: Observe, Request: holder})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		got, res, e := Transition(original, h.now, Command{Kind: Observe, Request: holder})
		if e != nil || !reflect.DeepEqual(got, data) || res != result {
			t.Fatal("map iteration changed scheduler output")
		}
	}
}

func TestTransitionUnrepresentableBackendTime(t *testing.T) {
	h := newStateHarness(t)
	r := stateTestRequest(h.now, stateTestConfig(), "a", 1)
	out, _, err := Transition(nil, time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), Command{Kind: Submit, Request: r})
	if !errors.Is(err, ErrCorruptState) || out != nil {
		t.Fatalf("unserializable time committed: %v", err)
	}
}

func TestTransitionRejectsCorruptLifecycleFields(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	cfg.Admission.MaxQueued = 2
	cfg.Admission.MaxQueuedPerTenant = 2
	released := stateTestRequest(h.now, cfg, "released", 3)
	lease := h.call(Submit, released, "", nil)
	h.call(Release, released, lease.LeaseID, nil)
	cancelled := stateTestRequest(h.now, cfg, "cancelled", 1)
	h.call(Cancel, cancelled, "", nil)
	expired := stateTestRequest(h.now, cfg, "expired", 1)
	expired.WaitUntil = h.now
	h.call(Submit, expired, "", nil)
	active := stateTestRequest(h.now, cfg, "active", 3)
	h.call(Submit, active, "", nil)
	queued := stateTestRequest(h.now, cfg, "queued", 1)
	h.call(Submit, queued, "", nil)
	queued2 := stateTestRequest(h.now, cfg, "queued", 1)
	h.call(Submit, queued2, "", nil)
	rejected := stateTestRequest(h.now, cfg, "rejected", 1)
	h.call(Submit, rejected, "", ErrQueueFull)
	baseline := append([]byte(nil), h.data...)
	mutations := map[string]func(*durableSnapshot){
		"zero revision":          func(s *durableSnapshot) { s.Revision = 0 },
		"zero snapshot time":     func(s *durableSnapshot) { s.Time = time.Time{} },
		"grant missing identity": func(s *durableSnapshot) { s.Records[active.ID].LeaseID = "" },
		"grant forged identity":  func(s *durableSnapshot) { s.Records[active.ID].LeaseID = leaseIdentity(queued) },
		"grant finished":         func(s *durableSnapshot) { s.Records[active.ID].Finished = h.now },
		"grant failure":          func(s *durableSnapshot) { s.Records[active.ID].Failure = ErrQueueFull.Error() },
		"grant at expiry":        func(s *durableSnapshot) { s.Records[active.ID].Until = h.now },
		"grant exceeds ttl":      func(s *durableSnapshot) { s.Records[active.ID].Until = h.now.Add(cfg.LeaseTTL + time.Millisecond) },
		"queue identity":         func(s *durableSnapshot) { s.Records[queued.ID].LeaseID = leaseIdentity(queued) },
		"queue finished":         func(s *durableSnapshot) { s.Records[queued.ID].Finished = h.now },
		"queue failure":          func(s *durableSnapshot) { s.Records[queued.ID].Failure = ErrQueueFull.Error() },
		"queue changed deadline": func(s *durableSnapshot) { s.Records[queued.ID].Until = queued.WaitUntil.Add(time.Millisecond) },
		"queue expired at snapshot": func(s *durableSnapshot) {
			s.Records[queued.ID].Request.WaitUntil = h.now
			s.Records[queued.ID].Until = h.now
		},
		"release missing identity": func(s *durableSnapshot) { s.Records[released.ID].LeaseID = "" },
		"release forged identity":  func(s *durableSnapshot) { s.Records[released.ID].LeaseID = leaseIdentity(queued) },
		"release zero finished":    func(s *durableSnapshot) { s.Records[released.ID].Finished = time.Time{} },
		"cancel future finished":   func(s *durableSnapshot) { s.Records[cancelled.ID].Finished = h.now.Add(time.Millisecond) },
		"expire failure":           func(s *durableSnapshot) { s.Records[expired.ID].Failure = ErrQueueFull.Error() },
		"reject empty failure":     func(s *durableSnapshot) { s.Records[rejected.ID].Failure = "" },
		"reject unknown failure":   func(s *durableSnapshot) { s.Records[rejected.ID].Failure = "not an admission error" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			var s durableSnapshot
			if err := json.Unmarshal(baseline, &s); err != nil {
				t.Fatal(err)
			}
			mutate(&s)
			data, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			out, _, err := Transition(data, h.now, Command{Kind: Observe, Request: active})
			if !errors.Is(err, ErrCorruptState) || out != nil {
				t.Fatalf("corrupt lifecycle accepted: output=%t error=%v", out != nil, err)
			}
		})
	}
}

func TestTransitionRemainingIncludesFractionalBackendTime(t *testing.T) {
	now := time.Unix(1000, 900000)
	r := stateTestRequest(now.Truncate(time.Millisecond), stateTestConfig(), "", 1)
	data, result, err := Transition(nil, now, Command{Kind: Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}
	if result.Remaining != r.Config.LeaseTTL-900*time.Microsecond {
		t.Fatalf("optimistic TTL: %s", result.Remaining)
	}
	_, result, err = Transition(data, now.Add(100*time.Millisecond), Command{Kind: Observe, Request: r})
	if err != nil || result.Remaining != r.Config.LeaseTTL-100*time.Millisecond-900*time.Microsecond {
		t.Fatalf("observation TTL: %s %v", result.Remaining, err)
	}
}

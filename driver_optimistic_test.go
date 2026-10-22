package airlock

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestCommitWindowBoundaries(t *testing.T) {
	now := time.Unix(1000, 123456000).UTC()
	window := CommitWindow{NotBefore: now, Before: now.Add(time.Millisecond)}
	for _, tc := range []struct {
		offset time.Duration
		want   bool
	}{{-time.Nanosecond, false}, {0, true}, {time.Millisecond - time.Nanosecond, true}, {time.Millisecond, false}, {time.Hour, false}} {
		if got := window.Allows(now.Add(tc.offset)); got != tc.want {
			t.Errorf("offset %v: got %t want %t", tc.offset, got, tc.want)
		}
	}
	window.Before = time.Time{}
	if !window.Allows(now.Add(100*365*24*time.Hour)) || window.Allows(now.Add(-time.Nanosecond)) {
		t.Fatal("unbounded window mishandles lower bound")
	}
}

func optimisticPlan(t *testing.T, data []byte, now time.Time, cmd Command) ([]byte, Result, CommitWindow, error) {
	t.Helper()
	next, res, window, err := TransitionOptimistic(data, now, cmd)
	reference, result, referenceErr := Transition(data, now, cmd)
	if !bytes.Equal(next, reference) || res != result || !errors.Is(err, referenceErr) {
		t.Fatalf("optimistic evaluation differs from serial transition: %v vs %v", err, referenceErr)
	}
	if next != nil && (!window.NotBefore.Equal(now) || window.NotBefore.Location() != time.UTC || !window.Allows(now)) {
		t.Fatalf("invalid evaluation window: %+v at %v", window, now)
	}
	return next, res, window, err
}

func TestOptimisticDirectGrantCannotCrossWaitDeadline(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	cfg := stateTestConfig()
	r := stateTestRequest(now, cfg, "direct", 1)
	r.WaitUntil = now.Add(2 * time.Millisecond)
	evaluated := now.Add(time.Millisecond + 123*time.Microsecond)
	cmd := Command{Kind: Submit, Request: r}
	_, res, window, err := optimisticPlan(t, nil, evaluated, cmd)
	if err != nil {
		t.Fatal(err)
	}
	expectState(t, res, Granted)
	if !window.Before.Equal(r.WaitUntil) || window.Allows(now.Add(3*time.Millisecond)) {
		t.Fatalf("direct grant crossed WaitUntil: %+v", window)
	}
	// A missed publication is discarded; retry from the unchanged authoritative snapshot.
	_, res, _, err = optimisticPlan(t, nil, now.Add(3*time.Millisecond), cmd)
	if err != nil {
		t.Fatal(err)
	}
	expectState(t, res, Expired)
}

func TestOptimisticFreshLeaseCannotBePublishedAlreadyExpired(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	cfg := stateTestConfig()
	cfg.LeaseTTL = 5 * time.Millisecond
	r := stateTestRequest(now, cfg, "direct", 1)
	cmd := Command{Kind: Submit, Request: r}
	_, res, window, err := optimisticPlan(t, nil, now, cmd)
	if err != nil {
		t.Fatal(err)
	}
	expectState(t, res, Granted)
	boundary := now.Add(cfg.LeaseTTL)
	if !window.Before.Equal(boundary) || window.Allows(boundary) {
		t.Fatalf("missing newly granted lease boundary: %+v", window)
	}
	// Re-evaluation may grant afresh, but computes a fresh lease deadline.
	_, res, window, err = optimisticPlan(t, nil, boundary, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if res.Remaining != cfg.LeaseTTL || !window.Before.Equal(boundary.Add(cfg.LeaseTTL)) {
		t.Fatal("miss reused expired candidate grant")
	}
}

func TestOptimisticRenewCannotCrossOriginalLeaseExpiry(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	r := stateTestRequest(h.now, cfg, "holder", 3)
	initial := h.call(Submit, r, "", nil)
	expiry := h.now.Add(cfg.LeaseTTL)
	h.now = expiry.Add(-time.Millisecond)
	cmd := Command{Kind: Renew, Request: r, LeaseID: initial.LeaseID}
	_, res, window, err := optimisticPlan(t, h.data, h.now, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if res.Remaining != cfg.LeaseTTL || !window.Before.Equal(expiry) || window.Allows(expiry) {
		t.Fatalf("renew bypassed original expiry: %+v %+v", res, window)
	}
	_, res, _, err = optimisticPlan(t, h.data, expiry, cmd)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired lease revived: %v", err)
	}
	expectState(t, res, Expired)
}

func TestOptimisticSubmissionTemporalBounds(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Request, time.Time)
		want   error
		status Status
	}{
		{"submit deadline", func(r *Request, now time.Time) {
			fresh := NewRequest(now.Add(5 * time.Millisecond))
			r.ID = fresh.ID
			r.SubmitUntil = fresh.SubmitUntil
		}, ErrSubmitExpired, ""},
		{"submit window opens", func(r *Request, now time.Time) {
			fresh := NewRequest(now.Add(r.Config.SubmitWindow + 5*time.Millisecond))
			r.ID = fresh.ID
			r.SubmitUntil = fresh.SubmitUntil
		}, nil, Granted},
		{"wait window opens", func(r *Request, now time.Time) { r.WaitUntil = now.Add(r.Config.MaxWait + 5*time.Millisecond) }, nil, Granted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			r := stateTestRequest(now, stateTestConfig(), "a", 1)
			tc.change(&r, now)
			cmd := Command{Kind: Submit, Request: r}
			_, _, window, err := optimisticPlan(t, nil, now, cmd)
			if tc.name == "submit deadline" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("future request accepted: %v", err)
			}
			boundary := now.Add(5 * time.Millisecond)
			if !window.Before.Equal(boundary) || window.Allows(boundary) {
				t.Fatalf("missing temporal boundary: %+v", window)
			}
			_, res, _, err := optimisticPlan(t, nil, boundary, cmd)
			if !errors.Is(err, tc.want) {
				t.Fatalf("recomputed error %v want %v", err, tc.want)
			}
			expectState(t, res, tc.status)
		})
	}
}

func TestOptimisticProtectedQueueExpiryRecomputesScheduling(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	cfg.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "high", Turns: 8, MaxQueued: 50}, {Name: "low", Turns: 1, MaxQueued: 50}}, DefaultGroup: "low", TenantGroups: map[string]string{"high": "high"}}
	holder := stateTestRequest(h.now, cfg, "holder", 2)
	h.call(Submit, holder, "", nil)
	big := stateTestRequest(h.now, cfg, "low", 3)
	big.WaitUntil = h.now.Add(500 * time.Millisecond)
	h.call(Submit, big, "", nil)
	high := stateTestRequest(h.now, cfg, "high", 1)
	h.call(Submit, high, "", nil)
	baseline := append([]byte(nil), h.data...)
	cursor := h.snapshot().Cursor
	cmd := Command{Kind: Observe, Request: high}
	for i := 0; i < 3; i++ {
		_, res, window, err := optimisticPlan(t, baseline, h.now, cmd)
		if err != nil {
			t.Fatal(err)
		}
		expectState(t, res, Queued)
		if !window.Before.Equal(big.WaitUntil) || window.Allows(big.WaitUntil) {
			t.Fatal("protected queue deadline missing")
		}
	}
	if !bytes.Equal(h.data, baseline) || h.snapshot().Cursor != cursor {
		t.Fatal("uncommitted attempts consumed scheduling state")
	}
	next, res, _, err := optimisticPlan(t, baseline, big.WaitUntil, cmd)
	if err != nil {
		t.Fatal(err)
	}
	expectState(t, res, Granted)
	h.data = next
	s := h.snapshot()
	if s.Records[big.ID].Status != Expired || s.Protected != "" {
		t.Fatal("expired protected request survived recomputation")
	}
	if s.Cursor != (cursor+1)%9 {
		t.Fatalf("high selection consumed wrong number of slots: %d -> %d", cursor, s.Cursor)
	}
}

func TestOptimisticGarbageCollectionBounds(t *testing.T) {
	for _, retention := range []time.Duration{time.Second, 20 * time.Second} {
		t.Run(retention.String(), func(t *testing.T) {
			h := newStateHarness(t)
			cfg := stateTestConfig()
			cfg.Retention = retention
			old := stateTestRequest(h.now, cfg, "old", 1)
			old.WaitUntil = old.SubmitUntil
			h.call(Cancel, old, "", nil)
			gcAt := old.SubmitUntil
			if candidate := h.now.Add(retention); candidate.After(gcAt) {
				gcAt = candidate
			}
			// Use an already expired, absent command so only retained history supplies a future bound.
			h.now = h.now.Add(2 * time.Second)
			probe := stateTestRequest(h.now, cfg, "probe", 1)
			id := NewRequest(h.now.Add(-time.Second))
			probe.ID = id.ID
			probe.SubmitUntil = id.SubmitUntil
			probe.WaitUntil = id.SubmitUntil
			cmd := Command{Kind: Observe, Request: probe}
			_, _, window, err := optimisticPlan(t, h.data, h.now, cmd)
			if !errors.Is(err, ErrSubmitExpired) {
				t.Fatal(err)
			}
			if !window.Before.Equal(gcAt) || window.Allows(gcAt) {
				t.Fatalf("missing terminal GC boundary: %+v want %v", window, gcAt)
			}
			next, _, window, err := optimisticPlan(t, h.data, gcAt, cmd)
			if !errors.Is(err, ErrSubmitExpired) {
				t.Fatal(err)
			}
			h.data = next
			if len(h.snapshot().Records) != 0 {
				t.Fatal("history not collected after missed guard")
			}
			if !window.Before.IsZero() {
				t.Fatalf("empty terminal operation has artificial upper bound: %+v", window)
			}
			_, _, _, err = optimisticPlan(t, h.data, gcAt, Command{Kind: Submit, Request: old})
			if !errors.Is(err, ErrSubmitExpired) {
				t.Fatal("old identity readmitted after GC")
			}
		})
	}
}

func TestOptimisticClockJumpAndInvalidState(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	r := stateTestRequest(h.now, cfg, "a", 1)
	h.call(Submit, r, "", nil)
	evaluated := h.now.Add(123 * time.Microsecond).In(time.FixedZone("offset", 3600))
	cmd := Command{Kind: Observe, Request: r}
	_, _, window, err := optimisticPlan(t, h.data, evaluated, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if window.Allows(evaluated.Add(-time.Microsecond)) {
		t.Fatal("submillisecond rollback allowed")
	}
	_, res, _, err := optimisticPlan(t, h.data, h.now.Add(2*time.Second), cmd)
	if err != nil {
		t.Fatal(err)
	}
	expectState(t, res, Expired)
	for _, tc := range []struct {
		name string
		data []byte
		now  time.Time
		cmd  Command
	}{
		{"bad snapshot", []byte("{"), h.now, cmd},
		{"backward stored time", h.data, h.now.Add(-time.Millisecond), cmd},
		{"invalid command", nil, h.now, Command{Kind: "invalid", Request: r}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next, _, window, err := optimisticPlan(t, tc.data, tc.now, tc.cmd)
			if err == nil || next != nil || window != (CommitWindow{}) {
				t.Fatalf("invalid evaluation produced a usable plan: %+v %v", window, err)
			}
		})
	}
}

package airlock

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestResizeTransition(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	r := stateTestRequest(h.now, cfg, "holder", 3)
	grant := h.call(Submit, r, "", nil)
	waiter := stateTestRequest(h.now, cfg, "queue", 3)
	h.call(Submit, waiter, "", nil)
	resize := func(capacity int, version uint64, want error) Result {
		t.Helper()
		data, result, window, err := transitionOptimistic(h.data, h.now, Command{Kind: Resize, Resize: ResizeRequest{Resource: r.Resource, Capacity: capacity, ExpectedVersion: version}})
		if !errors.Is(err, want) {
			t.Fatalf("Resize: %v want %v", err, want)
		}
		if data != nil {
			h.data = data
			if !window.Allows(h.now) || window.Before.IsZero() {
				t.Fatal(window)
			}
		}
		return result
	}
	out := resize(2, 1, nil)
	if out.ResourceStatus.Used != 3 || out.ResourceStatus.CapacityVersion != 2 {
		t.Fatal(out)
	}
	h.call(Observe, waiter, "", ErrWeightExceedsCapacity)
	h.call(Renew, r, grant.LeaseID, nil)
	h.call(Release, r, grant.LeaseID, nil)
	before := string(h.data)
	resize(4, 1, ErrCapacityVersionConflict)
	if string(h.data) != before {
		t.Fatal("conflict committed")
	}
	out = resize(4, 2, nil)
	if out.ResourceStatus.Capacity != 4 || out.ResourceStatus.CapacityVersion != 3 {
		t.Fatal(out)
	}
	large := stateTestRequest(h.now, cfg, "large", 4)
	h.call(Submit, large, "", nil)
	out = resize(4, 3, nil)
	if out.ResourceStatus.CapacityVersion != 3 {
		t.Fatal("noop bumped capacity version")
	}
}

func TestResizeValidationAndWindow(t *testing.T) {
	if (Command{Kind: Resize, Resize: ResizeRequest{Resource: "r"}}).Resource() != "r" || (Command{Request: Request{Resource: "b"}}).Resource() != "b" {
		t.Fatal("command target")
	}
	if next, _, _, err := Transition(nil, time.Now(), Command{Kind: Resize, Resize: ResizeRequest{Resource: "missing", Capacity: 1, ExpectedVersion: 1}}); next != nil || !errors.Is(err, ErrResourceNotFound) {
		t.Fatal(next, err)
	}
	for _, r := range []ResizeRequest{{}, {Resource: "x", Capacity: -1, ExpectedVersion: 1}, {Resource: "x", Capacity: 1}} {
		if ValidateResize(r) == nil {
			t.Fatal(r)
		}
	}
	if ValidateResource("bad\x00") == nil {
		t.Fatal("NUL accepted")
	}
	h := newStateHarness(t)
	cfg := stateTestConfig()
	r := stateTestRequest(h.now, cfg, "a", 1)
	h.call(Submit, r, "", nil)
	s := h.snapshot()
	s.CapacityVersion = ^uint64(0)
	cmd := Command{Kind: Resize, Resize: ResizeRequest{Resource: r.Resource, Capacity: 2, ExpectedVersion: s.CapacityVersion}}
	if next, _, _, err := Transition(&s, h.now, cmd); next != nil || !errors.Is(err, ErrCapacityVersionExhausted) {
		t.Fatal(err)
	}
	cmd.Resize.Capacity = s.Capacity
	if _, _, _, err := Transition(&s, h.now, cmd); err != nil {
		t.Fatal(err)
	}
	s = h.snapshot()
	cmd.Resize.Resource = "other"
	if _, _, _, err := Transition(&s, h.now, cmd); !errors.Is(err, ErrConfigConflict) {
		t.Fatal(err)
	}
	s = h.snapshot()
	s.Config.Retention = 0
	cmd.Resize.Resource = r.Resource
	cmd.Resize.ExpectedVersion = 1
	if _, _, _, err := Transition(&s, h.now, cmd); !errors.Is(err, ErrCorruptState) {
		t.Fatal(err)
	}
	// A resize expiring an existing grant and issuing another cannot publish
	// after either the original or the candidate's next business boundary.
	s = h.snapshot()
	cmd.Resize.Capacity = 4
	_, _, w, err := Transition(&s, h.now.Add(time.Millisecond), cmd)
	if err != nil || w.Before.After(h.now.Add(cfg.LeaseTTL)) {
		t.Fatal(w, err)
	}
}

type managementTestDriver struct {
	Driver
	inspect func(context.Context, string) (ResourceStatus, error)
	resize  func(context.Context, ResizeRequest) (ResourceStatus, error)
}

func (d managementTestDriver) InspectResource(ctx context.Context, r string) (ResourceStatus, error) {
	return d.inspect(ctx, r)
}
func (d managementTestDriver) Resize(ctx context.Context, r ResizeRequest) (ResourceStatus, error) {
	return d.resize(ctx, r)
}

func TestSharedResourceManagement(t *testing.T) {
	plain := clientNew(t, &clientStateDriver{}, clientOptions())
	if _, err := plain.Inspect(context.Background()); !errors.Is(err, ErrResourceManagementUnsupported) {
		t.Fatal(err)
	}
	if _, err := plain.Resize(context.Background(), 1, 1); !errors.Is(err, ErrResourceManagementUnsupported) {
		t.Fatal(err)
	}
	if _, err := plain.Resize(context.Background(), -1, 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := plain.Inspect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := plain.Resize(ctx, 1, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, mode := range []string{"ok", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := ResourceStatus{Capacity: 4, CapacityVersion: 2}
			call := func() (ResourceStatus, error) {
				if mode == "cancel" {
					cancel()
				}
				if mode == "error" {
					return ResourceStatus{}, ErrResourceNotFound
				}
				return want, nil
			}
			d := managementTestDriver{inspect: func(context.Context, string) (ResourceStatus, error) { return call() }, resize: func(context.Context, ResizeRequest) (ResourceStatus, error) { return call() }}
			shared := clientNew(t, d, clientOptions())
			_, err := shared.Inspect(ctx)
			if (mode == "ok" && err != nil) || (mode == "error" && !errors.Is(err, ErrResourceNotFound)) || (mode == "cancel" && !errors.Is(err, context.Canceled)) {
				t.Fatal(err)
			}
			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			_, err = shared.Resize(ctx, 4, 1)
			if (mode == "ok" && err != nil) || (mode == "error" && !errors.Is(err, ErrResourceNotFound)) || (mode == "cancel" && !errors.Is(err, context.Canceled)) {
				t.Fatal(err)
			}
		})
	}
}

func TestResourceInspectionValidation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	o := Observation{Resource: "x", Capacity: 1, CapacityVersion: 1, Revision: 1, Time: now}
	if _, err := o.Inspect(now, ""); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := o.Inspect(now, "other"); !errors.Is(err, ErrCorruptState) {
		t.Fatal(err)
	}
	if got, err := o.Inspect(now, "x"); err != nil || got.Capacity != 1 {
		t.Fatal(got, err)
	}
}

func TestResizeDoesNotDispatchAtOldCapacity(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	holder := stateTestRequest(h.now, cfg, "holder", 3)
	heavy := stateTestRequest(h.now, cfg, "heavy", 3)
	light := stateTestRequest(h.now, cfg, "light", 1)
	h.call(Submit, holder, "", nil)
	h.call(Submit, heavy, "", nil)
	h.call(Submit, light, "", nil)
	h.now = h.now.Add(cfg.LeaseTTL)
	next, result, _, err := transitionOptimistic(h.data, h.now, Command{Kind: Resize, Resize: ResizeRequest{Resource: holder.Resource, Capacity: 2, ExpectedVersion: 1}})
	if err != nil || result.ResourceStatus.Used != 1 {
		t.Fatal(result, err)
	}
	h.data = next
	s := h.snapshot()
	if s.Records[heavy.ID].Status != Rejected || s.Records[heavy.ID].LeaseID != "" || s.Records[light.ID].Status != Granted {
		t.Fatal("old-capacity dispatch occurred")
	}
	h.call(Observe, heavy, "", ErrWeightExceedsCapacity)
}

func TestResizePreservesRejectedIdentityAfterExpansion(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	r := stateTestRequest(h.now, cfg, "too-large", 4)
	h.call(Submit, r, "", ErrWeightExceedsCapacity)
	next, _, _, err := transitionOptimistic(h.data, h.now, Command{Kind: Resize, Resize: ResizeRequest{Resource: r.Resource, Capacity: 5, ExpectedVersion: 1}})
	if err != nil {
		t.Fatal(err)
	}
	h.data = next
	h.call(Submit, r, "", ErrWeightExceedsCapacity)
	fresh := stateTestRequest(h.now, cfg, "fresh", 4)
	expectState(t, h.call(Submit, fresh, "", nil), Granted)
}

func TestResizePreservesPriorityTurnAfterProtectedRejection(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	cfg.Admission.MaxQueued = 6
	cfg.Admission.MaxQueuedPerTenant = 6
	cfg.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 3}, {Name: "low", Turns: 1, MaxQueued: 3}}, DefaultGroup: "low", TenantGroups: map[string]string{"heavy": "high", "h1": "high", "h2": "high"}}
	holder := stateTestRequest(h.now, cfg, "holder", 2)
	grant := h.call(Submit, holder, "", nil)
	heavy := stateTestRequest(h.now, cfg, "heavy", 3)
	h1 := stateTestRequest(h.now, cfg, "h1", 1)
	h2 := stateTestRequest(h.now, cfg, "h2", 1)
	low := stateTestRequest(h.now, cfg, "low", 1)
	for _, r := range []Request{heavy, h1, h2, low} {
		expectState(t, h.call(Submit, r, "", nil), Queued)
	}
	if h.snapshot().Protected != heavy.ID {
		t.Fatal("missing protected heavy head")
	}
	next, _, _, err := transitionOptimistic(h.data, h.now, Command{Kind: Resize, Resize: ResizeRequest{Resource: holder.Resource, Capacity: 2, ExpectedVersion: 1}})
	if err != nil {
		t.Fatal(err)
	}
	h.data = next
	h.call(Release, holder, grant.LeaseID, nil)
	expectState(t, h.call(Observe, h1, "", nil), Granted)
	expectState(t, h.call(Observe, low, "", nil), Granted)
	expectState(t, h.call(Observe, h2, "", nil), Queued)
}

func TestNativeCapacityAndCheckedTotals(t *testing.T) {
	for _, tc := range []struct {
		used, weight, want int
		invalid            bool
	}{
		{0, maxInt, maxInt, false}, {maxInt - 1, 1, maxInt, false},
		{maxInt, 1, 0, true}, {-1, 1, 0, true}, {0, 0, 0, true}, {0, -1, 0, true},
	} {
		got, err := checkedWeight(tc.used, tc.weight)
		if got != tc.want || (err != nil) != tc.invalid {
			t.Fatal(tc, got, err)
		}
	}
	cfg := stateTestConfig()
	cfg.Admission.Capacity = maxInt
	if err := validConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := ValidateResize(ResizeRequest{Resource: "large", Capacity: maxInt, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	h := newStateHarness(t)
	large := stateTestRequest(h.now, cfg, "large", maxInt-1)
	small := stateTestRequest(h.now, cfg, "small", 1)
	if err := validRequest(large); err != nil {
		t.Fatal(err)
	}
	grant := h.call(Submit, large, "", nil)
	other := h.call(Submit, small, "", nil)
	waiting := stateTestRequest(h.now, cfg, "waiting", 1)
	expectState(t, h.call(Submit, waiting, "", nil), Queued)
	s := h.snapshot()
	o, err := s.Observation()
	if err != nil || o.Used != maxInt {
		t.Fatal(o, err)
	}
	if got, err := o.Inspect(h.now, large.Resource); err != nil || got.Capacity != maxInt || got.Used != maxInt {
		t.Fatal(got, err)
	}
	next, result, _, err := transitionOptimistic(h.data, h.now, Command{Kind: Resize, Resize: ResizeRequest{Resource: large.Resource, Capacity: 1, ExpectedVersion: 1}})
	if err != nil || result.ResourceStatus.Used != maxInt {
		t.Fatal(result, err)
	}
	h.data = next
	h.call(Renew, large, grant.LeaseID, nil)
	h.call(Release, large, grant.LeaseID, nil)
	expectState(t, h.call(Observe, waiting, "", nil), Queued)
	h.call(Release, small, other.LeaseID, nil)
	expectState(t, h.call(Observe, waiting, "", nil), Granted)
	// The count guard protects processing scale even at native-maximum capacity.
	cfg.MaxRecords = 1000001
	if !errors.Is(validConfig(cfg), ErrInvalidConfig) {
		t.Fatal("record bound removed")
	}
	cfg = stateTestConfig()
	cfg.Admission.Capacity = maxInt
	cfg.Admission.MaxQueued = 1000001
	if !errors.Is(validConfig(cfg), ErrInvalidConfig) {
		t.Fatal("queue bound removed")
	}
}

func TestOverflowingSnapshotAndProjectionFailClosed(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	cfg.Admission.Capacity = maxInt
	r := stateTestRequest(h.now, cfg, "a", maxInt)
	h.call(Submit, r, "", nil)
	s := h.snapshot()
	bad := stateTestRequest(h.now, cfg, "extra", 1)
	s.Records[bad.ID] = &DriverRecord{Request: bad, Status: Granted, LeaseID: leaseIdentity(bad), Until: h.now.Add(cfg.LeaseTTL)}
	if _, err := s.Observation(); !errors.Is(err, ErrCorruptState) {
		t.Fatal("projection overflow accepted", err)
	}
	if next, _, _, err := Transition(&s, h.now, Command{Kind: Observe, Request: r}); next != nil || !errors.Is(err, ErrCorruptState) {
		t.Fatal("restored sum overflow authorized", next, err)
	}
	if _, err := (*SharedState)(nil).Observation(); !errors.Is(err, ErrCorruptState) {
		t.Fatal(err)
	}
	if _, err := (&SharedState{Records: map[string]*DriverRecord{"bad": nil}}).Observation(); !errors.Is(err, ErrCorruptState) {
		t.Fatal(err)
	}
}

func TestSnapshotIntegerWidth(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	cfg.Admission.Capacity = maxInt
	request := stateTestRequest(h.now, cfg, "max", maxInt)
	h.call(Submit, request, "", nil)
	// The JSON codec must reject, rather than round or wrap, native max+1.
	tooLarge := []byte(strconv.FormatUint(uint64(maxInt)+1, 10))
	maximum := []byte(strconv.Itoa(maxInt))
	for _, field := range []string{"Capacity", "Weight"} {
		original := append([]byte(`"`+field+`":`), maximum...)
		replacement := append([]byte(`"`+field+`":`), tooLarge...)
		data := bytes.ReplaceAll(h.data, original, replacement)
		if bytes.Equal(data, h.data) {
			t.Fatalf("missing %s field", field)
		}
		if _, err := DecodeState(data); err == nil {
			t.Fatalf("accepted overflowing %s", field)
		}
	}
}

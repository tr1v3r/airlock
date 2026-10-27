package airlock

import (
	"context"
	"errors"
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
	for _, r := range []ResizeRequest{{}, {Resource: "x", Capacity: 0, ExpectedVersion: 1}, {Resource: "x", Capacity: 10001, ExpectedVersion: 1}, {Resource: "x", Capacity: 1}} {
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
	if _, err := plain.Resize(context.Background(), 0, 1); !errors.Is(err, ErrInvalidConfig) {
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

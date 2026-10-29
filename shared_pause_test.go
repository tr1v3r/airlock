package airlock

import (
	"errors"
	"testing"
)

func TestPausedStateValidationAndExpiry(t *testing.T) {
	h := newStateHarness(t)
	cfg := stateTestConfig()
	holder := stateTestRequest(h.now, cfg, "holder", 1)
	h.call(Submit, holder, "", nil)
	s := h.snapshot()
	next, _, _, err := Transition(&s, h.now, Command{Kind: Resize, Resize: ResizeRequest{Resource: holder.Resource, Capacity: 0, ExpectedVersion: 1}})
	if err != nil {
		t.Fatal(err)
	}
	pending := stateTestRequest(h.now, cfg, "pending", 100)
	next, out, _, err := Transition(next, h.now, Command{Kind: Submit, Request: pending})
	if err != nil || out.Status != Queued {
		t.Fatal(out, err)
	}
	obs, err := next.Observation()
	if err != nil {
		t.Fatal(err)
	}
	if status, e := obs.Inspect(h.now, holder.Resource); e != nil || status.Capacity != 0 || status.Queued != 1 {
		t.Fatal(status, e)
	}
	obs.Capacity = -1
	if _, e := obs.Inspect(h.now, holder.Resource); !errors.Is(e, ErrCorruptState) {
		t.Fatal(e)
	}
	bad := *next
	bad.Capacity = -1
	if _, _, _, e := Transition(&bad, h.now, Command{Kind: Observe, Request: pending}); !errors.Is(e, ErrCorruptState) {
		t.Fatal(e)
	}
	next, out, _, err = Transition(next, pending.WaitUntil, Command{Kind: Observe, Request: pending})
	if err != nil || out.Status != Expired {
		t.Fatal(out, err)
	}
	if next.Capacity != 0 || len(next.Queues) != 0 {
		t.Fatal(next)
	}
	if err := ValidateResize(ResizeRequest{Resource: holder.Resource, Capacity: 0, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
}

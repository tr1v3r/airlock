package protocol

import (
	"testing"
	"time"
)

func TestStructuredRenewRetainsOriginalCommitDeadline(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	cfg := stateTestConfig()
	cfg.LeaseTTL = 100 * time.Millisecond
	request := stateTestRequest(now, cfg, "renew", 1)
	state, grant, _, err := Transition(nil, now, Command{Kind: Submit, Request: request})
	if err != nil {
		t.Fatal(err)
	}
	originalExpiry := state.Records[request.ID].Until
	next, renewed, window, err := Transition(state, now.Add(50*time.Millisecond), Command{Kind: Renew, Request: request, LeaseID: grant.LeaseID})
	if err != nil {
		t.Fatal(err)
	}
	if next.Records[request.ID] != state.Records[request.ID] || !next.Records[request.ID].Until.After(originalExpiry) || renewed.Status != Granted {
		t.Fatal("expected in-place renewal of the exclusively owned record")
	}
	if !window.Before.Equal(originalExpiry) || window.Allows(originalExpiry) || !window.Allows(originalExpiry.Add(-time.Nanosecond)) {
		t.Fatalf("renewal lost its original expiry bound: %+v", window)
	}
}

func TestEncodeStateRejectsNil(t *testing.T) {
	if data, err := EncodeState(nil); data != nil || err != ErrCorruptState {
		t.Fatalf("nil transition output must not be persisted: %s %v", data, err)
	}
}

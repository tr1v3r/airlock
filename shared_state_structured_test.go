package airlock

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestTransitionStateStorageIndependence(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := NewRequest(now.Add(time.Second))
	r.Resource, r.Weight, r.WaitUntil = "structured", 1, now.Add(time.Second)
	r.Config = ResourceConfig{Admission: Config{Capacity: 1, MaxQueued: 2, MaxQueuedPerTenant: 2}, LeaseTTL: time.Second, MaxWait: time.Second, SubmitWindow: time.Second, Retention: time.Second, MaxRecords: 8}
	other := r
	id := NewRequest(r.SubmitUntil)
	other.ID, other.Owner = id.ID, id.Owner
	var state *SharedState
	var snapshot []byte
	var lease string
	for _, command := range []Command{{Kind: Submit, Request: r}, {Kind: Submit, Request: other}, {Kind: Observe, Request: other}, {Kind: Renew, Request: r}, {Kind: Release, Request: r}, {Kind: Cancel, Request: other}} {
		command.LeaseID = lease
		before := append([]byte(nil), snapshot...)
		next, result, err := transitionStructured(state, now, command)
		data, want, wantErr := transitionSnapshot(before, now, command)
		if !reflect.DeepEqual(result, want) || err != wantErr {
			t.Fatalf("%s: %+v %v != %+v %v", command.Kind, result, err, want, wantErr)
		}
		encoded, marshalErr := json.Marshal(next)
		if marshalErr != nil || string(encoded) != string(data) {
			t.Fatalf("structured/wire state mismatch: %s %s %v", encoded, data, marshalErr)
		}
		state, snapshot = next, data
		if command.Request.ID == r.ID {
			lease = result.LeaseID
		}
		now = now.Add(time.Millisecond)
	}
}

func TestTransitionStateRejectsInvalidInput(t *testing.T) {
	if next, result, err := transitionStructured(nil, time.Now(), Command{}); next != nil || result != (Result{}) || err != ErrInvalidConfig {
		t.Fatalf("%+v %+v %v", next, result, err)
	}
}

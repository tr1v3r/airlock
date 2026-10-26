package protocol

import (
	"encoding/json"
	"time"
)

// DecodeState parses an opaque JSON snapshot into structured state. A nil or
// empty snapshot is not valid here; ApplySnapshot treats it as a new resource.
// Malformed JSON fails with ErrCorruptState.
func DecodeState(snapshot []byte) (*State, error) {
	if len(snapshot) == 0 {
		return nil, ErrCorruptState
	}
	state := new(State)
	if json.Unmarshal(snapshot, state) != nil {
		return nil, ErrCorruptState
	}
	return state, nil
}

// EncodeState serializes structured state into the opaque JSON snapshot
// format. A nil state must never be persisted and fails with ErrCorruptState.
func EncodeState(state *State) ([]byte, error) {
	if state == nil {
		return nil, ErrCorruptState
	}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, ErrCorruptState
	}
	return data, nil
}

// ApplySnapshot adapts the reference state machine to opaque JSON snapshots:
// it decodes snapshot, applies cmd through ApplyCommand at backend time now,
// and re-encodes the next state. snapshot is nil or empty for a new resource;
// otherwise it must be the last committed snapshot for this resource. A nil
// snapshot argument yields a nil next state, which must not be persisted; a
// nonnil next state must be persisted atomically even alongside a business
// error. Drivers must not inspect or modify the opaque snapshot themselves.
func ApplySnapshot(snapshot []byte, now time.Time, cmd Command) ([]byte, Result, error) {
	if err := ValidateCommand(cmd); err != nil {
		return nil, Result{}, err
	}
	var state *State
	if len(snapshot) != 0 {
		state = new(State)
		if json.Unmarshal(snapshot, state) != nil {
			return nil, Result{}, ErrCorruptState
		}
	}
	next, result, err := ApplyCommand(state, now, cmd)
	if next == nil {
		return nil, result, err
	}
	data, e := json.Marshal(next)
	if e != nil {
		return nil, Result{}, ErrCorruptState
	}
	return data, result, err
}

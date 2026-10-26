package airlock

import (
	"encoding/json"
	"time"
)

// CommitWindow bounds publication under optimistic concurrency control.
// A driver must atomically compare the previously read state and enforce this
// window using backend time. Before is exclusive; zero means no upper bound.
// A confirmed comparison/window miss requires rereading and recomputing.
// Transport errors are ambiguous outcomes, not retryable CAS conflicts.
type CommitWindow struct{ NotBefore, Before time.Time }

// Allows reports whether backend time is inside the publication window.
func (w CommitWindow) Allows(now time.Time) bool {
	return !now.Before(w.NotBefore) && (w.Before.IsZero() || now.Before(w.Before))
}

// Transition advances the reference state machine without storage I/O.
// A nil state initializes a resource. The caller transfers exclusive ownership
// of state, including maps and records; input may be modified even on error.
// Persist every nonnil next state atomically, even when err is a business error.
// Never persist nil next or return a successful result before commit.
// Use backend time after locking, or enforce the returned window together with
// CAS. Lock-based drivers can ignore the window while retaining their lock.
func Transition(state *SharedState, now time.Time, cmd Command) (next *SharedState, result Result, window CommitWindow, err error) {
	window.NotBefore = now.UTC()
	boundary := func(at time.Time) {
		if at.After(now) && (window.Before.IsZero() || at.Before(window.Before)) {
			window.Before = at
		}
	}
	collect := func(s *SharedState) {
		if s == nil {
			return
		}
		for _, rec := range s.Records {
			if rec == nil {
				continue
			}
			if rec.Status == Queued || rec.Status == Granted {
				boundary(rec.Until)
				continue
			}
			at := rec.Finished.Add(s.Config.Retention)
			if rec.Request.SubmitUntil.After(at) {
				at = rec.Request.SubmitUntil
			}
			boundary(at)
		}
	}
	// Capture original expiry before renewal or sweeping mutates the input.
	collect(state)
	r := cmd.Request
	boundary(r.SubmitUntil)
	boundary(r.WaitUntil)
	boundary(r.SubmitUntil.Add(-r.Config.SubmitWindow))
	boundary(r.WaitUntil.Add(-r.Config.MaxWait))
	next, result, err = transition(state, now, cmd)
	if next == nil {
		return nil, result, CommitWindow{}, err
	}
	collect(next)
	return
}

// DecodeState decodes a snapshot. Empty input represents a new resource.
// Transition validates the decoded structure before using it.
func DecodeState(data []byte) (*SharedState, error) {
	if len(data) == 0 {
		return nil, nil
	}
	state := new(SharedState)
	if json.Unmarshal(data, state) != nil {
		return nil, ErrCorruptState
	}
	return state, nil
}

// EncodeState encodes a nonnil transition output for snapshot storage.
func EncodeState(state *SharedState) ([]byte, error) {
	if state == nil {
		return nil, ErrCorruptState
	}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, ErrCorruptState
	}
	return data, nil
}

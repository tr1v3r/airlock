package airlock

import (
	"encoding/json"
	"time"
)

// CommitWindow bounds publication when computation and commit use separate
// backend operations. Atomically enforce the window and either check current
// update-lock ownership or compare the previously read state. Before is exclusive;
// zero means no upper bound. Confirmed guard misses require rereading and
// recomputing; transport errors remain ambiguous outcomes.
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
// Use backend time after locking. A non-expiring lock held through commit permits
// ignoring the window; expiring leases require atomic owner/window checks at
// publication. Optimistic drivers must atomically compare prior state and window.
func Transition(state *SharedState, now time.Time, cmd Command) (next *SharedState, result Result, window CommitWindow, err error) {
	collector := commitWindowCollector{now: now, window: CommitWindow{NotBefore: now.UTC()}}
	next, result, err = transition(state, now, cmd, &collector)
	if next == nil {
		return nil, result, CommitWindow{}, err
	}
	return next, result, collector.window, err
}

// commitWindowCollector folds old/new record boundaries into existing state
// passes. now retains the original precision independently of scheduler time.
type commitWindowCollector struct {
	now    time.Time
	window CommitWindow
}

func (c *commitWindowCollector) boundary(at time.Time) {
	if at.After(c.now) && (c.window.Before.IsZero() || at.Before(c.window.Before)) {
		c.window.Before = at
	}
}

func (c *commitWindowCollector) request(r Request) {
	if c == nil {
		return
	}
	c.boundary(r.SubmitUntil)
	c.boundary(r.WaitUntil)
	c.boundary(r.SubmitUntil.Add(-r.Config.SubmitWindow))
	c.boundary(r.WaitUntil.Add(-r.Config.MaxWait))
}

func (c *commitWindowCollector) record(rec *DriverRecord, retention time.Duration) {
	if c == nil {
		return
	}
	if rec.Status == Queued || rec.Status == Granted {
		c.boundary(rec.Until)
		return
	}
	at := rec.Finished.Add(retention)
	if rec.Request.SubmitUntil.After(at) {
		at = rec.Request.SubmitUntil
	}
	c.boundary(at)
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

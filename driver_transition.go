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

// RetainedHistory describes terminal records deliberately omitted from a partial
// state. Count is exact, not an estimate: it excludes every omitted record whose
// max(Finished+Retention, SubmitUntil) is at or before now truncated to milliseconds.
// Before is the earliest such boundary among the counted records. It is zero if
// and only if Count is zero. These values must be read atomically with the state.
// Drivers must validate records on write/target reads and maintain an authoritative
// expiry index; this summary cannot validate omitted record contents itself.
type RetainedHistory struct {
	Count  int
	Before time.Time
}

// TransitionPartial advances the same scheduler and protocol as Transition while
// avoiding reads of unrelated retained terminal records. In addition to all
// scheduler metadata, state must contain EVERY queued/granted record and the
// command's existing record looked up by request ID, including a terminal record
// already due for GC. That last requirement preserves request-conflict detection
// before GC. Any other loaded terminal records are validated and processed normally.
// They must not also be included in history.Count. Records logically removed by
// an earlier committed GC are no longer existing records, even if a driver keeps
// physical garbage awaiting reclamation; do not resurrect them on target lookup.
//
// The returned state contains only the loaded records and new records; it is not
// a complete snapshot. Persist its changes together with the untouched history
// under the same atomic owner/version/window guard. Never pass this partial state
// to a driver or helper expecting a complete snapshot. For inspection, merge the
// terminal expiry index into NextMaintenance; omitted records do not affect usage.
// All Transition ownership, error-persistence and publication rules also apply.
// A nil state requires empty history. Existing Transition/EncodeState users need
// no changes and continue to operate on complete snapshots.
func TransitionPartial(state *SharedState, now time.Time, cmd Command, history RetainedHistory) (next *SharedState, result Result, window CommitWindow, err error) {
	if history.Count < 0 || (history.Count == 0 && !history.Before.IsZero()) ||
		(history.Count > 0 && (state == nil || !history.Before.After(now))) {
		return nil, Result{}, CommitWindow{}, ErrCorruptState
	}
	collector := commitWindowCollector{now: now, window: CommitWindow{NotBefore: now.UTC()}}
	collector.boundary(history.Before)
	next, result, err = transitionWithHistory(state, now, cmd, &collector, history.Count)
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

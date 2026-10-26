package protocol

import (
	"encoding/json"
	"time"
)

// CommitWindow bounds an optimistic transition's publication time. A backend
// must also atomically compare the exact previous snapshot before publishing.
// Before is exclusive; zero means no upper bound. NotBefore prevents publication
// after a backward clock jump. A miss requires a fresh read and recomputation.
// Transport errors are unknown outcomes, never compare-and-swap conflicts.
type CommitWindow struct {
	// NotBefore is inclusive; Before is exclusive and zero means no upper bound.
	NotBefore, Before time.Time
}

// Allows reports whether a backend time is inside the publication window.
func (w CommitWindow) Allows(now time.Time) bool {
	return !now.Before(w.NotBefore) && (w.Before.IsZero() || now.Before(w.Before))
}

// ApplyOptimistic evaluates the same state machine as ApplySnapshot and
// supplies the time guard needed by an optimistic compare-and-swap backend.
// The window is meaningful only for nonnil next. Only a confirmed comparison or
// time-window miss may be retried internally; a lost write response remains
// ambiguous, especially for Renew which has no independent operation identity.
// Drivers must not inspect or modify the opaque snapshot themselves.
func ApplyOptimistic(snapshot []byte, now time.Time, cmd Command) (next []byte, result Result, window CommitWindow, err error) {
	next, result, err = ApplySnapshot(snapshot, now, cmd)
	if next == nil {
		return
	}
	window.NotBefore = now.UTC()
	boundary := func(at time.Time) {
		if at.After(now) && (window.Before.IsZero() || at.Before(window.Before)) {
			window.Before = at
		}
	}
	r := cmd.Request
	// A direct grant does not retain WaitUntil as its lease expiry. Both command
	// deadlines must therefore participate even when no prior request is queued.
	boundary(r.SubmitUntil)
	boundary(r.WaitUntil)
	// Before these instants the submission can be too far in the future. Include
	// them so an optimistic error result cannot cross into a valid interval.
	boundary(r.SubmitUntil.Add(-r.Config.SubmitWindow))
	boundary(r.WaitUntil.Add(-r.Config.MaxWait))
	for _, data := range [][]byte{snapshot, next} {
		var s State
		// ApplySnapshot has already validated input and generated valid JSON output.
		_ = json.Unmarshal(data, &s)
		for _, rec := range s.Records {
			if rec.Status == Queued || rec.Status == Granted {
				boundary(rec.Until)
				continue
			}
			gcAt := rec.Finished.Add(s.Config.Retention)
			if rec.Request.SubmitUntil.After(gcAt) {
				gcAt = rec.Request.SubmitUntil
			}
			boundary(gcAt)
		}
	}
	return
}

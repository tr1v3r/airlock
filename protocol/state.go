package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Record is one request and its lifecycle state in the reference state
// machine. Request.Config is omitted in persisted state; State.Config is
// authoritative.
type Record struct {
	// Request is immutable; Config is omitted from persisted records and restored from State.
	Request Request
	// Status distinguishes pending/granted records from retained terminal outcomes.
	Status Status
	// LeaseID is derived on grant and retained after termination for owner-checked recovery.
	LeaseID string
	// Until is the exclusive queue or lease deadline for nonterminal records.
	Until time.Time
	// Finished starts terminal retention; it is zero while queued or granted.
	Finished time.Time
	// Failure stores a stable queue-rejection sentinel string only for Rejected records.
	Failure string
}

// State is the structured state of the reference durable scheduler.
// It is an optional implementation helper, not a storage format required by Driver.
// Version 1 uses per-tenant FIFO queues, group rotations and a protected head.
// Field names are the JSON keys of the persisted snapshot; renaming them breaks
// existing backend state.
type State struct {
	// Version identifies the scheduler state version; currently only version 1 is accepted.
	Version int
	// Resource binds the state to one logical capacity pool.
	Resource string
	// Config is the immutable resource policy shared by all retained records.
	Config ResourceConfig
	// Records maps request IDs to queued, granted and terminal outcomes within MaxRecords.
	Records map[string]*Record
	// Rotations preserves each distinct group's tenant selection order.
	Rotations [][]string
	// Queues preserves FIFO request IDs within each nonempty tenant queue.
	Queues map[string][]string
	// Protected is the selected request ID that cannot be bypassed, or empty.
	Protected string
	// Cursor is the next weighted group schedule position.
	Cursor int
	// Revision increments once for every published transition and must never wrap.
	Revision uint64
	// Time is the last millisecond backend evaluation time, used to reject clock regression.
	Time time.Time
}

// admissionError restores a known queue-rejection sentinel, rejecting unknown persisted strings.
func admissionError(s string) error {
	for _, e := range []error{ErrQueueFull, ErrGroupQueueFull, ErrTenantQueueFull} {
		if e.Error() == s {
			return e
		}
	}
	return ErrCorruptState
}

// grant starts a full lease at backend time and derives its stable identity.
func grant(r *Record, now time.Time) {
	r.Status = Granted
	r.Until = now.Add(r.Request.Config.LeaseTTL)
	r.LeaseID = leaseIdentity(r.Request)
}

// leaseIdentity derives a stable grant ID from request ID and owner; it is not a fencing counter.
func leaseIdentity(r Request) string {
	sum := sha256.Sum256([]byte(r.ID + ":" + r.Owner))
	return hex.EncodeToString(sum[:16])
}

// validRecord checks lifecycle consistency at the snapshot's last recorded backend time.
func validRecord(r *Record, at time.Time) bool {
	if r.LeaseID != "" && r.LeaseID != leaseIdentity(r.Request) {
		return false
	}
	switch r.Status {
	case Granted:
		return r.LeaseID != "" && r.Finished.IsZero() && r.Failure == "" && r.Until.After(at) && !r.Until.After(at.Add(r.Request.Config.LeaseTTL))
	case Queued:
		return r.LeaseID == "" && r.Finished.IsZero() && r.Failure == "" && r.Until.Equal(r.Request.WaitUntil) && r.Until.After(at)
	case Released, Expired, Cancelled, Rejected:
		return !r.Finished.IsZero() && !r.Finished.After(at) && (r.Status != Released || r.LeaseID != "") && ((r.Status == Rejected && admissionError(r.Failure) != ErrCorruptState) || (r.Status != Rejected && r.Failure == ""))
	default:
		return false
	}
}

package airlock

import (
	"time"

	"github.com/tr1v3r/airlock/protocol"
)

// Driver provides atomic admission operations for a resource. Implementations must
// serialize all operations, use backend time after obtaining exclusive access
// (or enforce the protocol.Transition publication window with atomic CAS),
// atomically persist lifecycle/scheduling changes even on business rejection,
// and honor ctx. Storage layout and use of the reference helpers are optional. Transport errors are ambiguous and never authorize work.
// Drivers do not own injected connections, execute work, or fall back locally.
// All callers of a resource must use the same immutable ResourceConfig.
// Implementations must be safe for concurrent use. Contexts must be non-nil.
// Request identities and owner secrets must be retained for recovery; never
// replace an ambiguous Submit with a fresh ID and assume no capacity was acquired.
type Driver = protocol.Driver

// ResourceConfig fixes admission, expiry and recovery bounds for a resource.
// Durations are positive whole milliseconds. LeaseTTL, MaxWait and SubmitWindow
// are at most one hour; Retention is at most 24 hours. MaxRecords bounds all
// queued, granted and terminal records together (1..10000). No records are
// evicted to admit new requests. Capacity and MaxQueued are at most 10000.
type ResourceConfig = protocol.ResourceConfig

// Request is an immutable submission, reused verbatim for every operation.
// Empty Tenant selects the default tenant. IDs encode SubmitUntil; changing the
// deadline requires a different ID. Owner is a fresh 128-bit random hex secret.
// Authenticate tenants and restrict backend access outside this package.
type Request = protocol.Request

// LeaseRef identifies a granted lease for Renew and Release. Request must be the
// unchanged original submission; ID is the LeaseID returned by a successful grant.
// It is an identity reference, not proof that the lease is still valid.
type LeaseRef = protocol.LeaseRef

// Operation identifies an atomic protocol transition.
type Operation = protocol.Operation

// Command is the input to the shared Transition implementation. Driver callers
// use the named methods with Request or LeaseRef instead of constructing commands.
type Command = protocol.Command

// Status is the authoritative lifecycle of a request.
type Status = protocol.Status

// Result never authorizes work alongside an error. Remaining is measured at the
// backend evaluation time; clients subtract the entire round trip. Revision is
// monotonic per resource, including maintenance-only transitions.
type Result = protocol.Result

const (
	// Submit admits a request or returns its existing outcome without refreshing TTL.
	Submit = protocol.Submit
	// Observe reads and advances expiry and scheduling; polling order is not grant order.
	Observe = protocol.Observe
	// Cancel terminates queued or granted requests, or creates a tombstone before Submit.
	Cancel = protocol.Cancel
	// Renew extends only a live matching lease, never a terminal request.
	Renew = protocol.Renew
	// Release ends a matching lease and returns its full weight exactly once.
	Release = protocol.Release
	// Queued means admitted to the persistent waiting queue.
	Queued = protocol.Queued
	// Granted means a lease currently owns its entire requested weight.
	Granted = protocol.Granted
	// Cancelled means Cancel won, including cancellation before Submit.
	Cancelled = protocol.Cancelled
	// Released means Release ended the lease.
	Released = protocol.Released
	// Expired means queue lifetime or lease lifetime ended.
	Expired = protocol.Expired
	// Rejected records a stable admission rejection for idempotent retries.
	Rejected = protocol.Rejected
)

// Protocol errors retain their identities across the client and state machine.
var (
	ErrInvalidRequest  = protocol.ErrInvalidRequest
	ErrConfigConflict  = protocol.ErrConfigConflict
	ErrRequestConflict = protocol.ErrRequestConflict
	ErrNotFound        = protocol.ErrNotFound
	ErrLeaseLost       = protocol.ErrLeaseLost
	ErrSubmitExpired   = protocol.ErrSubmitExpired
	ErrHistoryFull     = protocol.ErrHistoryFull
	ErrCorruptState    = protocol.ErrCorruptState
)

// NewRequest creates a cryptographically random identity bound to until. It does
// not submit anything or validate until. The time is normalized to UTC and
// truncated to milliseconds. Fill Resource, Tenant, Weight, WaitUntil and Config
// before submission; keep the completed request unchanged for recovery.
func NewRequest(until time.Time) Request { return protocol.NewRequest(until) }

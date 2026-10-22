package airlock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Driver provides atomic admission operations for a resource. Implementations must
// serialize all operations, use backend time after obtaining exclusive access
// (or enforce the TransitionOptimistic publication window with atomic CAS),
// persist the returned snapshot even when Transition returns a business error,
// and honor ctx. Transport errors are ambiguous and never authorize work.
// Drivers do not own injected connections, execute work, or fall back locally.
// All callers of a resource must use the same immutable ResourceConfig.
type Driver interface {
	// Submit admits or queues the whole request; retries do not refresh an existing lease.
	Submit(context.Context, Request) (Result, error)
	// Observe advances expiry and scheduling, then returns the request's current outcome.
	Observe(context.Context, Request) (Result, error)
	// Cancel terminates a queued or granted request, including cancellation before submission.
	Cancel(context.Context, Request) (Result, error)
	// Renew extends only a live lease matching the original request and lease ID.
	Renew(context.Context, LeaseRef) (Result, error)
	// Release returns a matching lease's whole weight at most once.
	Release(context.Context, LeaseRef) (Result, error)
}

// ResourceConfig fixes admission, expiry and recovery bounds for a resource.
// Durations are positive whole milliseconds. LeaseTTL, MaxWait and SubmitWindow
// are at most one hour; Retention is at most 24 hours. MaxRecords bounds all
// queued, granted and terminal records together (1..10000). No records are
// evicted to admit new requests. Capacity and MaxQueued are at most 10000.
type ResourceConfig struct {
	Admission    Config
	LeaseTTL     time.Duration
	MaxWait      time.Duration
	SubmitWindow time.Duration
	Retention    time.Duration
	MaxRecords   int
}

// Request is an immutable submission, reused verbatim for every operation.
// Empty Tenant selects the default tenant. IDs encode SubmitUntil; changing the
// deadline requires a different ID. Owner is a fresh 128-bit random hex secret.
// Authenticate tenants and restrict backend access outside this package.
type Request struct {
	Resource    string
	ID          string
	Owner       string
	SubmitUntil time.Time
	WaitUntil   time.Time
	Tenant      string
	Weight      int
	Config      ResourceConfig
}

// LeaseRef identifies a granted lease for Renew and Release. Request must be the
// unchanged original submission; ID is the LeaseID returned by a successful grant.
// It is an identity reference, not proof that the lease is still valid.
type LeaseRef struct {
	Request Request
	ID      string
}

// Operation identifies an atomic protocol transition.
type Operation string

const (
	// Submit admits a request or returns its existing outcome without refreshing TTL.
	Submit Operation = "submit"
	// Observe reads and advances expiry and scheduling; polling order is not grant order.
	Observe Operation = "observe"
	// Cancel terminates queued or granted requests, or creates a tombstone before Submit.
	Cancel Operation = "cancel"
	// Renew extends only a live matching lease, never a terminal request.
	Renew Operation = "renew"
	// Release ends a matching lease and returns its full weight exactly once.
	Release Operation = "release"
)

// Command is the input to the shared Transition implementation. Driver callers
// use the named methods with Request or LeaseRef instead of constructing commands.
type Command struct {
	Kind    Operation
	Request Request
	LeaseID string
}

// Status is the authoritative lifecycle of a request.
type Status string

const (
	// Queued means admitted to the persistent waiting queue.
	Queued Status = "queued"
	// Granted means a lease currently owns its entire requested weight.
	Granted Status = "granted"
	// Cancelled means Cancel won, including cancellation before Submit.
	Cancelled Status = "cancelled"
	// Released means Release ended the lease.
	Released Status = "released"
	// Expired means queue lifetime or lease lifetime ended.
	Expired Status = "expired"
	// Rejected records a stable admission rejection for idempotent retries.
	Rejected Status = "rejected"
)

// Result never authorizes work alongside an error. Remaining is measured at the
// backend evaluation time; clients subtract the entire round trip. Revision is
// monotonic per resource, including maintenance-only transitions.
type Result struct {
	Status    Status
	LeaseID   string
	Weight    int
	Remaining time.Duration
	Revision  uint64
}

// Protocol errors are stable identities; transport errors are preserved separately.
var (
	// ErrInvalidRequest reports malformed identifiers, deadlines, or operations.
	ErrInvalidRequest  = errors.New("airlock: invalid driver request")
	ErrConfigConflict  = errors.New("airlock: resource configuration conflict")
	ErrRequestConflict = errors.New("airlock: request identity or parameters conflict")
	ErrNotFound        = errors.New("airlock: request not found")
	ErrLeaseLost       = errors.New("airlock: lease no longer valid")
	ErrSubmitExpired   = errors.New("airlock: submission window expired")
	ErrHistoryFull     = errors.New("airlock: recovery history full")
	ErrCorruptState    = errors.New("airlock: invalid backend snapshot")
)

// NewRequest creates a cryptographically random identity bound to until. It does
// not submit anything. The caller supplies the other immutable request fields.
func NewRequest(until time.Time) Request {
	until = until.UTC().Truncate(time.Millisecond)
	return Request{ID: strconv.FormatInt(until.UnixMilli(), 10) + "-" + randomID(), Owner: randomID(), SubmitUntil: until}
}
func randomID() string { b := make([]byte, 16); _, _ = rand.Read(b); return hex.EncodeToString(b) }
func validSecret(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 16 && strings.ToLower(s) == s
}
func validConfig(c ResourceConfig) error {
	if c.Admission.Capacity > 10000 || c.Admission.MaxQueued > 10000 || c.MaxRecords < 1 || c.MaxRecords > 10000 {
		return ErrInvalidConfig
	}
	for _, d := range []time.Duration{c.LeaseTTL, c.MaxWait, c.SubmitWindow, c.Retention} {
		if d <= 0 || d%time.Millisecond != 0 {
			return ErrInvalidConfig
		}
	}
	if c.LeaseTTL > time.Hour || c.MaxWait > time.Hour || c.SubmitWindow > time.Hour || c.Retention > 24*time.Hour {
		return ErrInvalidConfig
	}
	if p := c.Admission.Priority; p != nil {
		if len(p.TenantGroups) > 10000 {
			return ErrInvalidConfig
		}
		for tenant, group := range p.TenantGroups {
			if len(tenant) > 256 || len(group) > 256 || !validText(tenant) || !validText(group) {
				return ErrInvalidConfig
			}
		}
		for _, group := range p.Groups {
			if len(group.Name) > 256 || !validText(group.Name) {
				return ErrInvalidConfig
			}
		}
	}
	_, err := New(c.Admission)
	return err
}
func validRequest(r Request) error {
	if err := validConfig(r.Config); err != nil {
		return err
	}
	prefix := strconv.FormatInt(r.SubmitUntil.UnixMilli(), 10) + "-"
	if r.Resource == "" || !validText(r.Resource) || !validText(r.Tenant) || len(r.Resource) > 256 || len(r.Tenant) > 256 || !validSecret(r.Owner) || !strings.HasPrefix(r.ID, prefix) || !validSecret(strings.TrimPrefix(r.ID, prefix)) || r.SubmitUntil.UnixMilli() <= 0 || !r.SubmitUntil.Equal(r.SubmitUntil.Truncate(time.Millisecond)) || !r.WaitUntil.Equal(r.WaitUntil.Truncate(time.Millisecond)) || r.WaitUntil.After(r.SubmitUntil.Add(r.Config.MaxWait)) {
		return ErrInvalidRequest
	}
	if r.Weight <= 0 {
		return ErrInvalidWeight
	}
	if r.Weight > r.Config.Admission.Capacity {
		return ErrWeightExceedsCapacity
	}
	return nil
}

// ValidateCommand checks immutable protocol input without reading backend state.
// Drivers should call it before allocating a resource or opening a transaction.
func ValidateCommand(cmd Command) error {
	if err := validRequest(cmd.Request); err != nil {
		return err
	}
	switch cmd.Kind {
	case Submit, Observe, Cancel, Renew, Release:
		return nil
	default:
		return ErrInvalidRequest
	}
}

func validText(s string) bool { return utf8.ValidString(s) && !strings.ContainsRune(s, 0) }

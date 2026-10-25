package protocol

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
// (or enforce the Transition publication window with atomic CAS),
// atomically persist lifecycle/scheduling changes even on business rejection,
// and honor ctx. Storage layout and use of the reference helpers are optional. Transport errors are ambiguous and never authorize work.
// Drivers do not own injected connections, execute work, or fall back locally.
// All callers of a resource must use the same immutable ResourceConfig.
// Implementations must be safe for concurrent use. Contexts must be non-nil.
// Request identities and owner secrets must be retained for recovery; never
// replace an ambiguous Submit with a fresh ID and assume no capacity was acquired.
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
	// Admission fixes weighted capacity and request queue limits; Capacity and MaxQueued are at most 10000.
	Admission Config
	// LeaseTTL is validity after grant or renewal, in whole milliseconds within (0, 1h].
	LeaseTTL time.Duration
	// MaxWait bounds new request waiting deadlines relative to backend time, within (0, 1h].
	MaxWait time.Duration
	// SubmitWindow bounds how far a new submission deadline may be in the future, within (0, 1h].
	SubmitWindow time.Duration
	// Retention keeps terminal outcomes after completion; GC also waits for SubmitUntil. Range: (0, 24h].
	Retention time.Duration
	// MaxRecords caps queued, granted and terminal history together, from 1 through 10000.
	MaxRecords int
}

// Request is an immutable submission, reused verbatim for every operation.
// Empty Tenant selects the default tenant. IDs encode SubmitUntil; changing the
// deadline requires a different ID. Owner is a fresh 128-bit random hex secret.
// Authenticate tenants and restrict backend access outside this package.
type Request struct {
	// Resource names the shared capacity pool: nonempty UTF-8 without NUL, at most 256 bytes.
	Resource string
	// ID binds a random secret to SubmitUntil; obtain it from NewRequest and reuse it unchanged.
	ID string
	// Owner is a lowercase 128-bit random hex secret created by NewRequest; retain it for recovery.
	Owner string
	// SubmitUntil is the exclusive first-submission deadline, in whole milliseconds, encoded in ID.
	SubmitUntil time.Time
	// WaitUntil is the exclusive queue deadline; grants use LeaseTTL instead. Use whole milliseconds.
	WaitUntil time.Time
	// Tenant is UTF-8 without NUL, at most 256 bytes; empty selects the shared default tenant.
	Tenant string
	// Weight is the atomic token count, from 1 through Config.Admission.Capacity.
	Weight int
	// Config must exactly match the immutable resolved policy used by all callers of Resource.
	Config ResourceConfig
}

// LeaseRef identifies a granted lease for Renew and Release. Request must be the
// unchanged original submission; ID is the LeaseID returned by a successful grant.
// It is an identity reference, not proof that the lease is still valid.
type LeaseRef struct {
	// Request is the unchanged original submission, including its owner secret and resolved config.
	Request Request
	// ID is the LeaseID returned by the original grant, not the request ID.
	ID string
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
	// Kind selects Submit, Observe, Cancel, Renew or Release.
	Kind Operation
	// Request identifies the original immutable submission for every operation.
	Request Request
	// LeaseID must match the grant for Renew/Release; other operations ignore it.
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
	// Status is the backend request lifecycle; only Granted with nil error may authorize work.
	Status Status
	// LeaseID identifies a grant and may remain on terminal records; it is not a fencing token.
	LeaseID string
	// Weight is the original allocation size; terminal records no longer hold those tokens.
	Weight int
	// Remaining is grant validity at backend evaluation; subtract the full round trip before use.
	Remaining time.Duration
	// Revision increases on committed transitions, including expiry/maintenance without a grant.
	Revision uint64
}

// Protocol errors are stable identities; transport errors are preserved separately.
var (
	// ErrInvalidRequest reports malformed identifiers, deadlines, or operations.
	ErrInvalidRequest = errors.New("airlock: invalid driver request")
	// ErrConfigConflict means the resource identity or immutable configuration differs from stored state.
	ErrConfigConflict = errors.New("airlock: resource configuration conflict")
	// ErrRequestConflict means a retained request ID was reused with a different owner or parameters.
	ErrRequestConflict = errors.New("airlock: request identity or parameters conflict")
	// ErrNotFound means no retained request exists for an operation that requires one.
	ErrNotFound = errors.New("airlock: request not found")
	// ErrLeaseLost means ownership is expired, mismatched, or no longer safe to use locally.
	ErrLeaseLost = errors.New("airlock: lease no longer valid")
	// ErrSubmitExpired rejects an unknown request whose first-submission deadline has passed.
	ErrSubmitExpired = errors.New("airlock: submission window expired")
	// ErrHistoryFull means retained records fill MaxRecords; existing outcomes are not evicted.
	ErrHistoryFull = errors.New("airlock: recovery history full")
	// ErrCorruptState rejects malformed or inconsistent snapshots, including a backward backend clock.
	ErrCorruptState = errors.New("airlock: invalid backend snapshot")
)

// NewRequest creates a cryptographically random identity bound to until. It does
// not submit anything or validate until. The time is normalized to UTC and
// truncated to milliseconds. Fill Resource, Tenant, Weight, WaitUntil and Config
// before submission; keep the completed request unchanged for recovery.
func NewRequest(until time.Time) Request {
	until = until.UTC().Truncate(time.Millisecond)
	return Request{ID: strconv.FormatInt(until.UnixMilli(), 10) + "-" + randomID(), Owner: randomID(), SubmitUntil: until}
}

// randomID generates a lowercase 128-bit cryptographic identity.
func randomID() string { b := make([]byte, 16); _, _ = rand.Read(b); return hex.EncodeToString(b) }

// validSecret accepts exactly 16 bytes encoded as lowercase hexadecimal.
func validSecret(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 16 && strings.ToLower(s) == s
}

// ValidateConfig checks protocol bounds and the local scheduler policy without backend I/O.
func ValidateConfig(c ResourceConfig) error {
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
	_, err := newScheduler(c.Admission)
	return err
}

// validRequest validates a complete immutable submission and its resource configuration.
func validRequest(r Request) error {
	if err := ValidateConfig(r.Config); err != nil {
		return err
	}
	return validRequestFields(r)
}

// validRequestFields requires an already validated Config. Restored records all
// share the immutable snapshot configuration; recompiling it per record would
// multiply restoration cost by the number of configured tenant mappings.
func validRequestFields(r Request) error {
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
// LeaseID matching and backend-relative deadline checks occur in Transition, not here.
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

// validText accepts shared-protocol strings that can be stored in PostgreSQL text and JSON.
func validText(s string) bool { return utf8.ValidString(s) && !strings.ContainsRune(s, 0) }

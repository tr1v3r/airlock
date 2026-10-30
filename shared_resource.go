package airlock

import (
	"context"
	"errors"
	"reflect"
	"time"
)

// ResourceManager is an optional Driver capability for existing resources.
// Resize must use the same atomic boundary as business commands. Implementations
// must never return success before publication or silently replay ambiguous writes.
type ResourceManager interface {
	InspectResource(context.Context, string) (ResourceStatus, error)
	Resize(context.Context, ResizeRequest) (ResourceStatus, error)
}

// ResizeRequest changes capacity without changing request identities or policy.
// Retry the same absolute Capacity and ExpectedVersion after ambiguous outcomes;
// never automatically adopt a newer version after a conflict.
// Capacity accepts any nonnegative Go int, whose width is process-dependent.
type ResizeRequest struct {
	Resource        string
	Capacity        int
	ExpectedVersion uint64
}

// ResourceStatus describes one committed snapshot, not executing business tasks.
// Used/Queued can include expired records not yet maintained. StateTime is the
// last evaluation time; Inspect does not sweep expiry or grant queued work.
type ResourceStatus struct {
	Resource        string
	Capacity        int
	Used            int
	Queued          int
	CapacityVersion uint64
	Revision        uint64
	StateTime       time.Time
}

var (
	// ErrResourceNotFound means management addressed an uninitialized resource.
	ErrResourceNotFound = errors.New("airlock: resource not found")
	// ErrCapacityVersionConflict rejects a stale management decision.
	ErrCapacityVersionConflict = errors.New("airlock: capacity version conflict")
	// ErrCapacityVersionExhausted prevents capacity version wraparound.
	ErrCapacityVersionExhausted = errors.New("airlock: capacity version exhausted")
	// ErrResourceManagementUnsupported means the Driver lacks ResourceManager.
	ErrResourceManagementUnsupported = errors.New("airlock: resource management unsupported")
)

// ValidateResource checks a resource name without storage I/O.
func ValidateResource(resource string) error {
	if resource == "" || len(resource) > 256 || !validText(resource) {
		return ErrInvalidRequest
	}
	return nil
}

// ValidateResize checks nonnegative int capacity and a mandatory expected version.
func ValidateResize(r ResizeRequest) error {
	if err := ValidateResource(r.Resource); err != nil {
		return err
	}
	if r.Capacity < 0 {
		return ErrInvalidConfig
	}
	if r.ExpectedVersion == 0 {
		return ErrInvalidRequest
	}
	return nil
}

// Observation builds a resource header from an exclusively owned committed state.
// Compute once at publication; attach one Record when answering Observe.
// Invalid nil records or an unrepresentable charged sum fail closed.
func (s *SharedState) Observation() (Observation, error) {
	if s == nil {
		return Observation{}, ErrCorruptState
	}
	o := Observation{Resource: s.Resource, Config: s.Config, Time: s.Time,
		Revision: s.Revision, Capacity: s.Capacity, CapacityVersion: s.CapacityVersion}
	for _, rec := range s.Records {
		if rec == nil {
			return Observation{}, ErrCorruptState
		}

		at := maintenanceAt(rec, s.Config.Retention)
		if o.NextMaintenance.IsZero() || at.Before(o.NextMaintenance) {
			o.NextMaintenance = at
		}
		if rec.Status == Granted {
			used, err := checkedWeight(o.Used, rec.Request.Weight)
			if err != nil {
				return Observation{}, err
			}

			o.Used = used
		}
		if rec.Status == Queued {
			o.Queued++
		}
	}
	return o, nil
}

// ResourceStatus extracts the committed counters without doing maintenance.
func (o Observation) ResourceStatus() ResourceStatus {
	return ResourceStatus{Resource: o.Resource, Capacity: o.Capacity, Used: o.Used,
		Queued: o.Queued, CapacityVersion: o.CapacityVersion, Revision: o.Revision, StateTime: o.Time}
}

// Inspect validates one atomically read resource header using backend time.
func (o Observation) Inspect(now time.Time, resource string) (ResourceStatus, error) {
	if err := ValidateResource(resource); err != nil {
		return ResourceStatus{}, err
	}
	if o.Resource != resource || o.Capacity < 0 || o.CapacityVersion == 0 ||
		o.Revision == 0 || o.Revision == ^uint64(0) || o.Time.IsZero() || now.Truncate(time.Millisecond).Before(o.Time) ||
		o.Used < 0 || o.Queued < 0 || o.Queued > maxCountLimit {
		return ResourceStatus{}, ErrCorruptState
	}
	return o.ResourceStatus(), nil
}

// Inspect reads a resource without advancing expiry or waiting for holders.
func (d *Shared) Inspect(ctx context.Context) (ResourceStatus, error) {
	if err := ctx.Err(); err != nil {
		return ResourceStatus{}, err
	}
	manager, ok := d.driver.(ResourceManager)
	if !ok {
		return ResourceStatus{}, ErrResourceManagementUnsupported
	}
	ctx, cancel := context.WithTimeout(ctx, d.options.OperationTimeout)
	defer cancel()
	result, err := manager.InspectResource(ctx, d.options.Resource)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ResourceStatus{}, ctxErr
	}
	if err != nil {
		return ResourceStatus{}, err
	}
	return result, nil
}

// Resize changes capacity with optimistic management-version protection.
// Zero pauses admissions without rejecting queues; a positive resize resumes
// admissions and rejects queued weights exceeding the new capacity.
// It never revokes grants, waits for drain, or changes queue/lease policy.
// A matching same-capacity call maintains state but does not change CapacityVersion.
func (d *Shared) Resize(ctx context.Context, capacity int, expectedVersion uint64) (ResourceStatus, error) {
	request := ResizeRequest{Resource: d.options.Resource, Capacity: capacity, ExpectedVersion: expectedVersion}
	if err := ValidateResize(request); err != nil {
		return ResourceStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return ResourceStatus{}, err
	}
	manager, ok := d.driver.(ResourceManager)
	if !ok {
		return ResourceStatus{}, ErrResourceManagementUnsupported
	}
	ctx, cancel := context.WithTimeout(ctx, d.options.OperationTimeout)
	defer cancel()
	result, err := manager.Resize(ctx, request)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ResourceStatus{}, ctxErr
	}
	if err != nil {
		return ResourceStatus{}, err
	}
	return result, nil
}

// maxInt is the arithmetic boundary of the existing int-based public API.
// Capacity/weight values do not allocate one record per token.
const maxInt = int(^uint(0) >> 1)

// checkedWeight rejects malformed restored/projection totals before adding.
// Live grants instead check weight <= capacity-used before addition; both
// operands are nonnegative ints, so that comparison and the resulting sum fit.
func checkedWeight(used, weight int) (int, error) {
	if used < 0 || weight <= 0 || weight > maxInt-used {
		return 0, ErrCorruptState
	}
	return used + weight, nil
}

// Observation is a compact, atomically read projection of committed resource
// metadata and one request. Drivers may use it to avoid a full Observe transition
// while no expiry or history maintenance is due. Never construct it from an
// independently cached request and resource revision.
type Observation struct {
	Resource        string
	Config          ResourceConfig
	Capacity        int
	CapacityVersion uint64
	Used            int
	Queued          int
	Time            time.Time
	Revision        uint64
	NextMaintenance time.Time
	Record          *DriverRecord
}

// NextMaintenance returns the earliest queue/lease expiry or history collection
// boundary in an already validated transition output. Zero means no timed work.
// Compute it once per committed transition, not once per waiting request.
func (s *SharedState) NextMaintenance() time.Time {
	var next time.Time

	for _, rec := range s.Records {
		at := maintenanceAt(rec, s.Config.Retention)

		if next.IsZero() || at.Before(next) {
			next = at
		}
	}

	return next
}

// Observe returns a request result without modifying state when no maintenance
// is due. A false ready means the driver must run its ordinary atomic Observe;
// missing records also fall back to preserve submission-window/error semantics.
// A successful fast read does not increase Revision. Use authoritative backend
// time from the same atomic read and subtract the full round trip from Remaining.
func (o Observation) Observe(now time.Time, request Request) (result Result, ready bool, err error) {
	if err := ValidateCommand(Command{Kind: Observe, Request: request}); err != nil {
		return Result{}, true, err
	}

	request.SubmitUntil = request.SubmitUntil.UTC()
	request.WaitUntil = request.WaitUntil.UTC()
	if o.Resource != request.Resource || !o.Config.EqualPolicy(request.Config) {
		return Result{}, true, ErrConfigConflict
	}
	if o.Revision == 0 || o.Revision == ^uint64(0) || o.Time.IsZero() || now.Truncate(time.Millisecond).Before(o.Time) {
		return Result{}, true, ErrCorruptState
	}
	if !o.NextMaintenance.IsZero() && !now.Before(o.NextMaintenance) {
		return Result{}, false, nil
	}
	if o.Record == nil {
		return Result{}, false, nil
	}

	rec := *o.Record
	rec.Request.Config = request.Config
	if !reflect.DeepEqual(rec.Request, request) {
		return Result{}, true, ErrRequestConflict
	}
	if validRequestFields(rec.Request) != nil || !validRecord(&rec, o.Time) {
		return Result{}, true, ErrCorruptState
	}
	// Protect against an inconsistent projection that omitted this record's deadline.
	if (rec.Status == Granted || rec.Status == Queued) && !now.Before(rec.Until) {
		return Result{}, false, nil
	}

	result = Result{Status: rec.Status, LeaseID: rec.LeaseID, Weight: rec.Request.Weight, Revision: o.Revision}
	if rec.Status == Granted {
		result.Remaining = rec.Until.Sub(now)
	}

	if rec.Status == Rejected {
		err = admissionError(rec.Failure)
	}
	return result, true, err
}

// maintenanceAt is the record's next expiry or terminal-retention boundary.
func maintenanceAt(rec *DriverRecord, retention time.Duration) time.Time {
	if rec.Status == Queued || rec.Status == Granted {
		return rec.Until
	}
	at := rec.Finished.Add(retention)
	if rec.Request.SubmitUntil.After(at) {
		at = rec.Request.SubmitUntil
	}
	return at
}

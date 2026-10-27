package airlock

import (
	"reflect"
	"time"
)

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
		at := rec.Until
		if rec.Status != Queued && rec.Status != Granted {
			at = rec.Finished.Add(s.Config.Retention)
			if rec.Request.SubmitUntil.After(at) {
				at = rec.Request.SubmitUntil
			}
		}

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

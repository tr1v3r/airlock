package airlock

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"time"
)

// DriverRecord is one request and its lifecycle state in the reference scheduler.
// Request.Config is omitted in persisted state; SharedState.Config is authoritative.
type DriverRecord struct {
	// Request is immutable; Config is omitted from persisted records and restored from SharedState.
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

// SharedState is the structured state of the reference durable scheduler.
// It is an optional implementation helper, not a storage format required by Driver.
// Version 1 uses per-tenant FIFO queues, group rotations and a protected head.
type SharedState struct {
	// Version identifies the scheduler state version; currently only version 1 is accepted.
	Version int
	// Resource binds the state to one logical capacity pool.
	Resource string
	// Config holds immutable rules and the original creation-capacity hint.
	// Current Capacity is independent; EqualPolicy excludes the hint.
	Config ResourceConfig
	// Capacity is the current admission limit, independent of Config's creation hint.
	Capacity int
	// CapacityVersion changes only when capacity changes; starts at one.
	CapacityVersion uint64
	// Records maps request IDs to queued, granted and terminal outcomes within MaxRecords.
	Records map[string]*DriverRecord
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

type durableRecord = DriverRecord
type durableSnapshot = SharedState

// transition advances exclusively owned structured state.
func transition(state *SharedState, now time.Time, cmd Command, window *commitWindowCollector) (*SharedState, Result, error) {
	r := cmd.Request
	r.SubmitUntil = r.SubmitUntil.UTC()
	r.WaitUntil = r.WaitUntil.UTC()
	cmd.Request = r
	if err := ValidateCommand(cmd); err != nil {
		return nil, Result{}, err
	}

	observedNow := now.UTC()
	now = observedNow.Truncate(time.Millisecond)
	var s SharedState
	if state == nil {
		if cmd.Kind == Resize {
			return nil, Result{}, ErrResourceNotFound
		}
		s = SharedState{Version: 1, Resource: r.Resource, Config: r.Config, Capacity: r.Config.Admission.Capacity, CapacityVersion: 1, Records: make(map[string]*durableRecord), Queues: make(map[string][]string)}
	} else {
		s = *state
	}

	if state != nil && (s.Version != 1 || s.Records == nil || s.Revision == 0 || s.Revision == ^uint64(0) ||
		s.Time.IsZero() || now.Before(s.Time) || s.Capacity < 0 || s.CapacityVersion == 0) {
		return nil, Result{}, ErrCorruptState
	}
	if cmd.Kind == Resize {
		if s.Resource != cmd.Resize.Resource {
			return nil, Result{}, ErrConfigConflict
		}
		if err := validConfig(s.Config); err != nil {
			return nil, Result{}, ErrCorruptState
		}
		if s.CapacityVersion != cmd.Resize.ExpectedVersion {
			return nil, Result{}, ErrCapacityVersionConflict
		}
		if s.Capacity != cmd.Resize.Capacity && s.CapacityVersion == ^uint64(0) {
			return nil, Result{}, ErrCapacityVersionExhausted
		}
	} else {
		if s.Resource != r.Resource || !s.Config.EqualPolicy(r.Config) {
			return nil, Result{}, ErrConfigConflict
		}
		r.Config = s.Config
		cmd.Request = r
	}

	for _, rec := range s.Records {
		if rec != nil {
			// Capture original deadlines before expiry, renewal or cancellation.
			window.record(rec, s.Config.Retention)
			rec.Request.Config = s.Config
		}
	}

	// Keep boundary order: original records, original command, final records.
	if cmd.Kind != Resize {
		window.request(cmd.Request)
	}

	if old := s.Records[r.ID]; cmd.Kind != Resize && old != nil && !reflect.DeepEqual(old.Request, r) {
		return nil, Result{}, ErrRequestConflict
	}

	g, waiters, err := s.restore()
	if err != nil {
		return nil, Result{}, err
	}

	// Expire all old grants/queues before granting anything. Map iteration does
	// not alter survivor order; dispatch runs only after the entire sweep.
	for id, rec := range s.Records {
		if (rec.Status == Queued || rec.Status == Granted) && !now.Before(rec.Until) {
			if rec.Status == Queued {
				g.remove(waiters[id])
				delete(waiters, id)
			} else {
				g.used -= rec.Request.Weight
				g.active--
			}

			rec.Status = Expired
			rec.Finished = now
		}

		if rec.Status != Queued && rec.Status != Granted && !now.Before(rec.Finished.Add(s.Config.Retention)) && !now.Before(rec.Request.SubmitUntil) {
			delete(s.Records, id)
		}
	}

	var rec *durableRecord
	var opErr error
	if cmd.Kind == Resize {
		if s.Capacity != cmd.Resize.Capacity {
			s.CapacityVersion++
		}
		s.Capacity = cmd.Resize.Capacity
		g.cfg.Capacity = s.Capacity
		for id, w := range waiters {
			if s.Capacity > 0 && w.weight > s.Capacity {
				g.remove(w)
				delete(waiters, id)
				record := s.Records[id]
				record.Status, record.Finished, record.Failure = Rejected, now, ErrWeightExceedsCapacity.Error()
			}
		}
	} else {
		s.dispatch(g, waiters, now)
		rec = s.Records[r.ID]
		if rec == nil {
			switch {
			case !now.Before(r.SubmitUntil):
				opErr = ErrSubmitExpired
			case cmd.Kind != Submit && cmd.Kind != Cancel:
				opErr = ErrNotFound
			case r.SubmitUntil.After(now.Add(r.Config.SubmitWindow)) || r.WaitUntil.After(now.Add(r.Config.MaxWait)):
				opErr = ErrInvalidRequest
			case len(s.Records) >= s.Config.MaxRecords:
				opErr = ErrHistoryFull
			default:
				rec = &durableRecord{Request: r}
				s.Records[r.ID] = rec

				switch {
				case cmd.Kind == Cancel:
					rec.Status = Cancelled
					rec.Finished = now
				case cmd.Kind == Submit && s.Capacity > 0 && r.Weight > s.Capacity:
					rec.Status = Rejected
					rec.Finished = now
					rec.Failure = ErrWeightExceedsCapacity.Error()
				case !now.Before(r.WaitUntil):
					rec.Status = Expired
					rec.Finished = now
				case g.queued == 0 && r.Weight <= g.cfg.Capacity-g.used:
					g.used += r.Weight
					g.active++
					grant(rec, now)
				default:
					w, e := g.enqueue(r.Tenant, r.Weight)
					if e != nil {
						rec.Status = Rejected
						rec.Finished = now
						rec.Failure = e.Error()
					} else {
						rec.Status = Queued
						rec.Until = r.WaitUntil
						waiters[r.ID] = w
					}
				}
			}
		} else {
			switch cmd.Kind {
			case Cancel:
				if rec.Status == Queued {
					g.remove(waiters[r.ID])
					delete(waiters, r.ID)
				}

				if rec.Status == Granted {
					g.used -= r.Weight
					g.active--
				}

				if rec.Status == Queued || rec.Status == Granted {
					rec.Status = Cancelled
					rec.Finished = now
				}

			case Renew, Release:
				if rec.LeaseID == "" || rec.LeaseID != cmd.LeaseID {
					opErr = ErrLeaseLost
					break
				}

				if rec.Status == Granted {
					if cmd.Kind == Renew {
						rec.Until = now.Add(r.Config.LeaseTTL)
					} else {
						g.used -= r.Weight
						g.active--
						rec.Status = Released
						rec.Finished = now
					}
				} else if cmd.Kind == Renew {
					opErr = ErrLeaseLost
				}
			}
		}

	}

	s.dispatch(g, waiters, now)
	s.capture(g, waiters)
	s.Time = now
	s.Revision++
	out := Result{Revision: s.Revision}
	if cmd.Kind == Resize {
		observation, err := s.Observation()
		if err != nil {
			return nil, Result{}, err
		}
		out.ResourceStatus = observation.ResourceStatus()
	}
	if rec != nil {
		out.Status = rec.Status
		out.LeaseID = rec.LeaseID
		out.Weight = rec.Request.Weight
		if rec.Status == Granted {
			out.Remaining = rec.Until.Sub(observedNow)
		}

		if rec.Status == Rejected && opErr == nil {
			opErr = admissionError(rec.Failure)
		}
	}

	// Store immutable configuration once, not once per retained request.
	for _, rec := range s.Records {
		// All lifecycle/scheduling changes are complete; collect final deadlines.
		window.record(rec, s.Config.Retention)
		rec.Request.Config = ResourceConfig{}
	}

	return &s, out, opErr
}

// admissionError restores a known queue-rejection sentinel, rejecting unknown persisted strings.
func admissionError(s string) error {
	for _, e := range []error{ErrQueueFull, ErrGroupQueueFull, ErrTenantQueueFull, ErrWeightExceedsCapacity} {
		if e.Error() == s {
			return e
		}
	}

	return ErrCorruptState
}

// grant starts a full lease at backend time and derives its stable identity.
func grant(r *durableRecord, now time.Time) {
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
func validRecord(r *durableRecord, at time.Time) bool {
	if r.LeaseID != "" && r.LeaseID != leaseIdentity(r.Request) {
		return false
	}

	switch r.Status {
	case Granted:
		return r.LeaseID != "" && r.Finished.IsZero() && r.Failure == "" && r.Until.After(at) && !r.Until.After(at.Add(r.Request.Config.LeaseTTL))
	case Queued:
		return r.LeaseID == "" && r.Finished.IsZero() && r.Failure == "" && r.Until.Equal(r.Request.WaitUntil) && r.Until.After(at)
	case Released, Expired, Cancelled, Rejected:
		return !r.Finished.IsZero() && !r.Finished.After(at) && (r.Status != Released || r.LeaseID != "") &&
			((r.Status == Rejected && admissionError(r.Failure) != ErrCorruptState) || (r.Status != Rejected && r.Failure == ""))
	default:
		return false
	}
}

// dispatch applies shared scheduling and converts newly granted waiters into durable leases.
func (s *durableSnapshot) dispatch(g *Airlock, ws map[string]*waiter, now time.Time) {
	g.dispatch()

	for id, w := range ws {
		if w.granted {
			grant(s.Records[id], now)
			delete(ws, id)
		}
	}
}

// groups deduplicates the weighted schedule in configured group order.
func groups(g *Airlock) []*priorityQueue {
	var result []*priorityQueue
	seen := make(map[*priorityQueue]bool)

	for _, q := range g.priority.schedule {
		if !seen[q] {
			seen[q] = true
			result = append(result, q)
		}
	}

	return result
}

// capture saves tenant FIFO order, group rotations and the protected request for later restoration.
func (s *durableSnapshot) capture(g *Airlock, ws map[string]*waiter) {
	ids := make(map[*waiter]string, len(ws))

	for id, w := range ws {
		ids[w] = id
	}

	s.Protected = ids[g.protected]
	s.Cursor = g.priority.cursor
	s.Rotations = nil
	s.Queues = make(map[string][]string)

	for _, group := range groups(g) {
		tenants := []string{}

		for e := group.rotation.Front(); e != nil; e = e.Next() {
			tq := e.Value.(*tenantQueue)
			tenants = append(tenants, tq.id)

			for n := tq.requests.Front(); n != nil; n = n.Next() {
				s.Queues[tq.id] = append(s.Queues[tq.id], ids[n.Value.(*waiter)])
			}
		}

		s.Rotations = append(s.Rotations, tenants)
	}
}

// restore receives the Config validated by ValidateCommand and matched against
// the snapshot by Transition. Each record is assigned that same Config before
// restoration; only its identity, deadlines, weight and lifecycle need checking.
func (s *durableSnapshot) restore() (*Airlock, map[string]*waiter, error) {
	admission := s.Config.Admission
	g, err := New(admission)
	if err != nil {
		return nil, nil, ErrCorruptState
	}

	g.cfg.Capacity = s.Capacity // Creation stays positive; restored resources may be paused.
	ws := make(map[string]*waiter)
	qs := groups(g)
	if s.Cursor < 0 || s.Cursor >= len(g.priority.schedule) || (len(s.Rotations) != 0 &&
		len(s.Rotations) != len(qs)) || len(s.Records) > s.Config.MaxRecords {
		return nil, nil, ErrCorruptState
	}

	for id, r := range s.Records {
		if r == nil || r.Request.ID != id || r.Request.Resource != s.Resource ||
			validRequestFields(r.Request) != nil || !reflect.DeepEqual(r.Request.Config, s.Config) || !validRecord(r, s.Time) {
			return nil, nil, ErrCorruptState
		}

		switch r.Status {
		case Granted:
			used, err := checkedWeight(g.used, r.Request.Weight)
			if err != nil {
				return nil, nil, err
			}

			g.used = used
			g.active++
		case Queued, Cancelled, Released, Expired, Rejected:
		}
	}

	for i, rotation := range s.Rotations {
		for _, tenant := range rotation {
			if g.tenants[tenant] != nil || g.priority.group(tenant) != qs[i] || len(s.Queues[tenant]) == 0 {
				return nil, nil, ErrCorruptState
			}

			for _, id := range s.Queues[tenant] {
				r := s.Records[id]
				if r == nil || r.Status != Queued || (s.Capacity > 0 && r.Request.Weight > s.Capacity) || r.Request.Tenant != tenant || ws[id] != nil {
					return nil, nil, ErrCorruptState
				}

				w, e := g.enqueue(tenant, r.Request.Weight)
				if e != nil {
					return nil, nil, ErrCorruptState
				}

				ws[id] = w
			}
		}
	}

	for id, r := range s.Records {
		if r.Status == Queued && ws[id] == nil {
			return nil, nil, ErrCorruptState
		}
	}

	if len(g.tenants) != len(s.Queues) {
		return nil, nil, ErrCorruptState
	}

	if s.Protected != "" {
		g.protected = ws[s.Protected]
		if g.protected == nil || g.protected.tenant.requests.Front().Value != g.protected {
			return nil, nil, ErrCorruptState
		}
	}

	g.priority.cursor = s.Cursor
	return g, ws, nil
}

package protocol

import (
	"reflect"
	"time"

	"github.com/tr1v3r/airlock/internal/scheduler"
)

// ApplyCommand advances the reference state machine by one command. state is
// nil for a new resource; otherwise it must be the last committed output for
// this resource. The caller transfers exclusive ownership of state and its
// maps/records; input may be modified even on error and must not be reused.
// Persist every nonnil returned state atomically, including on business
// errors. A nil return must not be persisted. No I/O occurs.
//
// Drivers must invoke it inside their atomic boundary, use backend time
// obtained after locking, and return a result only after commit. Backward
// clock movement fails closed. State/expiry work is bounded by
// ResourceConfig.MaxRecords (<=10000). SQL drivers may store records
// separately and write only changed rows; Driver implementations need not use
// this reference machine. The process-local Airlock shares the same enqueue,
// removal and dispatch logic through the internal scheduler.
//
// The flow is: validate and restore state → expire stale records and reclaim
// history → schedule already-queued waiters → execute the command → schedule
// again → save the scheduling position and produce the result.
func ApplyCommand(state *State, now time.Time, cmd Command) (*State, Result, error) {
	// Stage 1: normalize and validate the immutable command, including its config.
	r := cmd.Request
	r.SubmitUntil = r.SubmitUntil.UTC()
	r.WaitUntil = r.WaitUntil.UTC()
	cmd.Request = r
	if err := ValidateCommand(cmd); err != nil {
		return nil, Result{}, err
	}
	observedNow := now.UTC()
	now = observedNow.Truncate(time.Millisecond)
	// Stage 2: adopt the structured state; a nil state initializes the resource.
	var s State
	if state == nil {
		s = State{Version: 1, Resource: r.Resource, Config: r.Config, Records: make(map[string]*Record), Queues: make(map[string][]string)}
	} else {
		s = *state
	}
	if state != nil && (s.Version != 1 || s.Records == nil || s.Revision == 0 || s.Revision == ^uint64(0) || s.Time.IsZero() || now.Before(s.Time)) {
		return nil, Result{}, ErrCorruptState
	}
	if s.Resource != r.Resource || !reflect.DeepEqual(s.Config, r.Config) {
		return nil, Result{}, ErrConfigConflict
	}
	for _, rec := range s.Records {
		if rec != nil {
			rec.Request.Config = s.Config
		}
	}
	if old := s.Records[r.ID]; old != nil && !reflect.DeepEqual(old.Request, r) {
		return nil, Result{}, ErrRequestConflict
	}
	// Stage 3: recover the scheduling core and the waiters of queued records.
	g, waiters, err := s.restore()
	if err != nil {
		return nil, Result{}, err
	}
	// Stage 4: expire all old grants/queues and reclaim finished history before
	// granting anything. Map iteration does not alter survivor order; dispatch
	// runs only after the entire sweep.
	for id, rec := range s.Records {
		if (rec.Status == Queued || rec.Status == Granted) && !now.Before(rec.Until) {
			if rec.Status == Queued {
				g.Remove(waiters[id])
				delete(waiters, id)
			} else {
				g.Release(rec.Request.Weight)
			}
			rec.Status = Expired
			rec.Finished = now
		}
		if rec.Status != Queued && rec.Status != Granted && !now.Before(rec.Finished.Add(s.Config.Retention)) && !now.Before(rec.Request.SubmitUntil) {
			delete(s.Records, id)
		}
	}
	// Stage 5: schedule already-queued waiters, converting grants into leases.
	s.dispatch(g, waiters, now)
	// Stage 6: execute the command on the target record.
	rec := s.Records[r.ID]
	var opErr error
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
			rec = &Record{Request: r}
			s.Records[r.ID] = rec
			switch {
			case cmd.Kind == Cancel:
				rec.Status = Cancelled
				rec.Finished = now
			case !now.Before(r.WaitUntil):
				rec.Status = Expired
				rec.Finished = now
			case g.TryAcquire(r.Weight):
				grant(rec, now)
			default:
				w, e := g.Enqueue(r.Tenant, r.Weight)
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
				g.Remove(waiters[r.ID])
				delete(waiters, r.ID)
			}
			if rec.Status == Granted {
				g.Release(r.Weight)
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
					g.Release(r.Weight)
					rec.Status = Released
					rec.Finished = now
				}
			} else if cmd.Kind == Renew {
				opErr = ErrLeaseLost
			}
		}
	}
	// Stage 7: schedule again, including any capacity freed by the command.
	s.dispatch(g, waiters, now)
	// Stage 8: save the scheduling position, bump the revision and build the result.
	s.capture(g, waiters)
	s.Time = now
	s.Revision++
	out := Result{Revision: s.Revision}
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
		rec.Request.Config = ResourceConfig{}
	}
	return &s, out, opErr
}

// dispatch applies shared scheduling and converts newly granted waiters into durable leases.
func (s *State) dispatch(g *scheduler.Scheduler, ws map[string]*scheduler.Waiter, now time.Time) {
	g.Dispatch()
	for id, w := range ws {
		if w.Granted() {
			grant(s.Records[id], now)
			delete(ws, id)
		}
	}
}

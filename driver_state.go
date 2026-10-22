package airlock

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"
)

type durableRecord struct {
	Request  Request
	Status   Status
	LeaseID  string
	Until    time.Time
	Finished time.Time
	Failure  string
}
type durableSnapshot struct {
	Version   int
	Resource  string
	Config    ResourceConfig
	Records   map[string]*durableRecord
	Rotations [][]string
	Queues    map[string][]string
	Protected string
	Cursor    int
	Revision  uint64
	Time      time.Time
}

// Transition is the reference durable state machine. snapshot is nil for a new
// resource; otherwise it must be the last committed output for this resource.
// Drivers must invoke it inside their atomic boundary, persist nonnil output
// even on business errors, and return a result only after commit. No I/O occurs.
// Snapshots are versioned opaque JSON, not a caller-editable configuration API.
// Time must come from the backend after locking. Backward clock movement fails
// closed. State/expiry work is bounded by ResourceConfig.MaxRecords (<=10000).
// Existing local Airlocks and this machine share enqueue/remove/dispatch logic.
func Transition(snapshot []byte, now time.Time, cmd Command) ([]byte, Result, error) {
	r := cmd.Request
	r.SubmitUntil = r.SubmitUntil.UTC()
	r.WaitUntil = r.WaitUntil.UTC()
	cmd.Request = r
	if err := ValidateCommand(cmd); err != nil {
		return nil, Result{}, err
	}
	now = now.UTC().Truncate(time.Millisecond)
	var s durableSnapshot
	if len(snapshot) == 0 {
		s = durableSnapshot{Version: 1, Resource: r.Resource, Config: r.Config, Records: make(map[string]*durableRecord), Queues: make(map[string][]string)}
	} else if json.Unmarshal(snapshot, &s) != nil || s.Version != 1 || s.Records == nil || s.Revision == 0 || s.Revision == ^uint64(0) || s.Time.IsZero() || now.Before(s.Time) {
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
	s.dispatch(g, waiters, now)
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
			rec = &durableRecord{Request: r}
			s.Records[r.ID] = rec
			switch {
			case cmd.Kind == Cancel:
				rec.Status = Cancelled
				rec.Finished = now
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
	s.dispatch(g, waiters, now)
	s.capture(g, waiters)
	s.Time = now
	s.Revision++
	out := Result{Revision: s.Revision}
	if rec != nil {
		out.Status = rec.Status
		out.LeaseID = rec.LeaseID
		out.Weight = rec.Request.Weight
		if rec.Status == Granted {
			out.Remaining = rec.Until.Sub(now)
		}
		if rec.Status == Rejected && opErr == nil {
			opErr = admissionError(rec.Failure)
		}
	}
	// Store immutable configuration once, not once per retained request.
	for _, rec := range s.Records {
		rec.Request.Config = ResourceConfig{}
	}
	data, e := json.Marshal(s)
	if e != nil {
		return nil, Result{}, ErrCorruptState
	}
	return data, out, opErr
}
func admissionError(s string) error {
	for _, e := range []error{ErrQueueFull, ErrGroupQueueFull, ErrTenantQueueFull} {
		if e.Error() == s {
			return e
		}
	}
	return ErrCorruptState
}
func grant(r *durableRecord, now time.Time) {
	r.Status = Granted
	r.Until = now.Add(r.Request.Config.LeaseTTL)
	r.LeaseID = leaseIdentity(r.Request)
}
func leaseIdentity(r Request) string {
	sum := sha256.Sum256([]byte(r.ID + ":" + r.Owner))
	return hex.EncodeToString(sum[:16])
}
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
		return !r.Finished.IsZero() && !r.Finished.After(at) && (r.Status != Released || r.LeaseID != "") && ((r.Status == Rejected && admissionError(r.Failure) != ErrCorruptState) || (r.Status != Rejected && r.Failure == ""))
	default:
		return false
	}
}
func (s *durableSnapshot) dispatch(g *Airlock, ws map[string]*waiter, now time.Time) {
	g.dispatch()
	for id, w := range ws {
		if w.granted {
			grant(s.Records[id], now)
			delete(ws, id)
		}
	}
}
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
func (s *durableSnapshot) restore() (*Airlock, map[string]*waiter, error) {
	g, err := New(s.Config.Admission)
	if err != nil {
		return nil, nil, ErrCorruptState
	}
	ws := make(map[string]*waiter)
	qs := groups(g)
	if s.Cursor < 0 || s.Cursor >= len(g.priority.schedule) || (len(s.Rotations) != 0 && len(s.Rotations) != len(qs)) || len(s.Records) > s.Config.MaxRecords {
		return nil, nil, ErrCorruptState
	}
	for id, r := range s.Records {
		if r == nil || r.Request.ID != id || r.Request.Resource != s.Resource || validRequest(r.Request) != nil || !reflect.DeepEqual(r.Request.Config, s.Config) || !validRecord(r, s.Time) {
			return nil, nil, ErrCorruptState
		}
		switch r.Status {
		case Granted:
			g.used += r.Request.Weight
			g.active++
		case Queued, Cancelled, Released, Expired, Rejected:
		}
	}
	if g.used > g.cfg.Capacity {
		return nil, nil, ErrCorruptState
	}
	for i, rotation := range s.Rotations {
		for _, tenant := range rotation {
			if g.tenants[tenant] != nil || g.priority.group(tenant) != qs[i] || len(s.Queues[tenant]) == 0 {
				return nil, nil, ErrCorruptState
			}
			for _, id := range s.Queues[tenant] {
				r := s.Records[id]
				if r == nil || r.Status != Queued || r.Request.Tenant != tenant || ws[id] != nil {
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

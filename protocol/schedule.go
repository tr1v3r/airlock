package protocol

import (
	"reflect"

	"github.com/tr1v3r/airlock/internal/scheduler"
)

// restore rebuilds the scheduling core and the record-to-waiter mapping from
// the structured state. It receives the Config validated by ValidateCommand
// and matched against the snapshot by ApplyCommand. Each record is assigned
// that same Config before restoration; only its identity, deadlines, weight
// and lifecycle need checking. Any inconsistency fails with ErrCorruptState.
func (s *State) restore() (*scheduler.Scheduler, map[string]*scheduler.Waiter, error) {
	g, err := scheduler.New(s.Config.Admission)
	if err != nil {
		return nil, nil, ErrCorruptState
	}
	ws := make(map[string]*scheduler.Waiter)
	if s.Cursor < 0 || s.Cursor >= g.ScheduleSlots() || (len(s.Rotations) != 0 && len(s.Rotations) != g.GroupCount()) || len(s.Records) > s.Config.MaxRecords {
		return nil, nil, ErrCorruptState
	}
	for id, r := range s.Records {
		if r == nil || r.Request.ID != id || r.Request.Resource != s.Resource || validRequestFields(r.Request) != nil || !reflect.DeepEqual(r.Request.Config, s.Config) || !validRecord(r, s.Time) {
			return nil, nil, ErrCorruptState
		}
		if r.Status == Granted {
			g.Reserve(r.Request.Weight)
		}
	}
	if g.Stats().InUse > g.Capacity() {
		return nil, nil, ErrCorruptState
	}
	for i, rotation := range s.Rotations {
		for _, tenant := range rotation {
			if g.HasQueue(tenant) || g.GroupIndex(tenant) != i || len(s.Queues[tenant]) == 0 {
				return nil, nil, ErrCorruptState
			}
			for _, id := range s.Queues[tenant] {
				r := s.Records[id]
				if r == nil || r.Status != Queued || r.Request.Tenant != tenant || ws[id] != nil {
					return nil, nil, ErrCorruptState
				}
				w, e := g.Enqueue(tenant, r.Request.Weight)
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
	if g.Stats().WaitingTenants != len(s.Queues) {
		return nil, nil, ErrCorruptState
	}
	if s.Protected != "" && !g.SetProtected(ws[s.Protected]) {
		return nil, nil, ErrCorruptState
	}
	if !g.SetCursor(s.Cursor) {
		return nil, nil, ErrCorruptState
	}
	return g, ws, nil
}

// capture saves tenant FIFO order, group rotations and the protected request
// for later restoration. Waiters are correlated back to their request IDs.
func (s *State) capture(g *scheduler.Scheduler, ws map[string]*scheduler.Waiter) {
	ids := make(map[*scheduler.Waiter]string, len(ws))
	for id, w := range ws {
		ids[w] = id
	}
	layout := g.Layout()
	s.Protected = ids[layout.Protected]
	s.Cursor = layout.Cursor
	s.Rotations = nil
	s.Queues = make(map[string][]string)
	for _, group := range layout.Rotations {
		tenants := []string{}
		for _, tenant := range group {
			tenants = append(tenants, tenant)
			for _, w := range layout.Queues[tenant] {
				s.Queues[tenant] = append(s.Queues[tenant], ids[w])
			}
		}
		s.Rotations = append(s.Rotations, tenants)
	}
}

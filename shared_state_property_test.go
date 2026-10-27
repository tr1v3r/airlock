package airlock

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// protocolModel deliberately uses a single tenant and a slice FIFO, not Airlock,
// restore, enqueue or dispatch. It checks lifecycle/GC/accounting over changing
// traces; the separate local scheduler model and deterministic priority tests
// cover cross-tenant/group selection. Lease identity is opaque to this model:
// leaseIdentity is used only to construct a matching reference, never to decide
// whether a grant, renewal, expiry or release is allowed.
type protocolModelRecord struct {
	request  Request
	status   Status
	until    time.Time
	finished time.Time
	leased   bool
	failure  error
}

type protocolModel struct {
	cfg      ResourceConfig
	records  map[string]*protocolModelRecord
	queue    []string
	revision uint64
}

func (m *protocolModel) inUse() int {
	used := 0

	for _, r := range m.records {
		if r.status == Granted {
			used += r.request.Weight
		}
	}

	return used
}

func (m *protocolModel) drain(now time.Time) {
	var live []string

	for _, id := range m.queue {
		if r := m.records[id]; r != nil && r.status == Queued {
			live = append(live, id)
		}
	}

	m.queue = live
	used := m.inUse()

	for len(m.queue) > 0 {
		r := m.records[m.queue[0]]
		if r.request.Weight > m.cfg.Admission.Capacity-used {
			break
		}

		r.status, r.leased, r.until = Granted, true, now.Add(m.cfg.LeaseTTL)
		used += r.request.Weight
		m.queue = m.queue[1:]
	}
}

func (m *protocolModel) apply(now time.Time, cmd Command) (Status, error, bool) {
	r := cmd.Request
	if old := m.records[r.ID]; old != nil && !reflect.DeepEqual(old.request, r) {
		return "", ErrRequestConflict, false
	}

	for id, old := range m.records {
		if (old.status == Granted || old.status == Queued) && !now.Before(old.until) {
			old.status, old.finished = Expired, now
		}

		if old.status != Granted && old.status != Queued && !now.Before(old.finished.Add(m.cfg.Retention)) && !now.Before(old.request.SubmitUntil) {
			delete(m.records, id)
		}
	}

	m.drain(now)
	old := m.records[r.ID]
	var err error
	if old == nil {
		switch {
		case !now.Before(r.SubmitUntil):
			err = ErrSubmitExpired
		case cmd.Kind != Submit && cmd.Kind != Cancel:
			err = ErrNotFound
		case r.SubmitUntil.After(now.Add(m.cfg.SubmitWindow)) || r.WaitUntil.After(now.Add(m.cfg.MaxWait)):
			err = ErrInvalidRequest
		case len(m.records) == m.cfg.MaxRecords:
			err = ErrHistoryFull
		default:
			old = &protocolModelRecord{request: r}
			m.records[r.ID] = old

			switch {
			case cmd.Kind == Cancel:
				old.status, old.finished = Cancelled, now
			case !now.Before(r.WaitUntil):
				old.status, old.finished = Expired, now
			case len(m.queue) == 0 && r.Weight <= m.cfg.Admission.Capacity-m.inUse():
				old.status, old.leased, old.until = Granted, true, now.Add(m.cfg.LeaseTTL)
			case len(m.queue) == m.cfg.Admission.MaxQueued:
				old.status, old.finished, old.failure = Rejected, now, ErrQueueFull
			default:
				old.status, old.until = Queued, r.WaitUntil
				m.queue = append(m.queue, r.ID)
			}
		}
	} else {
		switch cmd.Kind {
		case Cancel:
			if old.status == Granted || old.status == Queued {
				old.status, old.finished = Cancelled, now
			}
		case Renew, Release:
			if !old.leased || cmd.LeaseID != leaseIdentity(old.request) {
				err = ErrLeaseLost
			} else if old.status == Granted {
				if cmd.Kind == Renew {
					old.until = now.Add(m.cfg.LeaseTTL)
				} else {
					old.status, old.finished = Released, now
				}
			} else if cmd.Kind == Renew {
				err = ErrLeaseLost
			}
		}
	}

	m.drain(now)
	m.revision++
	if old == nil {
		return "", err, true
	}

	if old.failure != nil && err == nil {
		err = old.failure
	}
	return old.status, err, true
}

func (m *protocolModel) check(t testing.TB, data []byte) {
	t.Helper()
	var s durableSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	if s.Revision != m.revision || len(s.Records) != len(m.records) {
		t.Fatalf("model revision/size: got %d/%d want %d/%d", s.Revision, len(s.Records), m.revision, len(m.records))
	}

	for id, expected := range m.records {
		got := s.Records[id]
		if got == nil || got.Status != expected.status || !got.Until.Equal(expected.until) ||
			!got.Finished.Equal(expected.finished) || (got.LeaseID != "") != expected.leased {
			t.Fatalf("model record %s: got %+v want %+v", id, got, expected)
		}

		wantFailure := ""
		if expected.failure != nil {
			wantFailure = expected.failure.Error()
		}

		if got.Failure != wantFailure {
			t.Fatalf("failure: got %q want %q", got.Failure, wantFailure)
		}

		got.Request.Config = m.cfg
		if !reflect.DeepEqual(got.Request, expected.request) {
			t.Fatalf("record identity/weight changed: got %+v want %+v", got.Request, expected.request)
		}
	}

	if got := s.Queues["tenant"]; !reflect.DeepEqual(append([]string{}, got...), append([]string{}, m.queue...)) {
		t.Fatalf("queue mismatch: got %v want %v", got, m.queue)
	}
	if used := m.inUse(); used < 0 || used > m.cfg.Admission.Capacity {
		t.Fatalf("model capacity violated: %d", used)
	}
}

func runProtocolTrace(t testing.TB, trace []byte, priority bool) {
	t.Helper()
	cfg := ResourceConfig{Admission: Config{Capacity: 7, MaxQueued: 5, MaxQueuedPerTenant: 5}, LeaseTTL: 10 * time.Millisecond, MaxWait: 40 * time.Millisecond, SubmitWindow: 80 * time.Millisecond, Retention: 20 * time.Millisecond, MaxRecords: 12}
	if priority {
		cfg.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "all", Turns: 3, MaxQueued: 5}}, DefaultGroup: "all", TenantGroups: map[string]string{"tenant": "all"}}
	}

	m := protocolModel{cfg: cfg, records: make(map[string]*protocolModelRecord)}
	now := time.Unix(3000, 0).UTC()
	var data []byte
	var requests []Request

	for step, instruction := range trace {
		// Separate byte bits choose elapsed time, operation, identity reuse and
		// weight. Zero advances allow saturated queues, later jumps exercise GC.
		now = now.Add(time.Duration(instruction>>6) * 3 * time.Millisecond)
		kind := []Operation{Submit, Submit, Observe, Cancel, Renew, Release}[int(instruction)%6]
		var r Request
		if len(requests) == 0 || instruction&8 == 0 {
			until := now.Add(cfg.SubmitWindow)
			r = Request{Resource: "model", ID: fmt.Sprintf("%d-%032x", until.UnixMilli(), step+1), Owner: fmt.Sprintf("%032x", step+1), SubmitUntil: until, WaitUntil: now.Add(cfg.MaxWait), Tenant: "tenant", Weight: 1 + int(instruction>>4)%7, Config: cfg}
			requests = append(requests, r)
		} else {
			r = requests[int(instruction)%len(requests)]
		}

		cmd := Command{Kind: kind, Request: r, LeaseID: leaseIdentity(r)}
		if instruction&32 != 0 {
			cmd.LeaseID = "wrong"
		}

		if instruction == 255 {
			cmd.Request.Owner = "00000000000000000000000000000000"
		}

		wantStatus, wantErr, commit := m.apply(now, cmd)
		next, result, _, err := equivalentWindowTransition(t, data, now, cmd)
		if !errors.Is(err, wantErr) || result.Status != wantStatus || (next != nil) != commit {
			t.Fatalf("step %d instruction=%d kind=%s: status=%s err=%v committed=%t; want %s/%v/%t", step, instruction, kind, result.Status, err, next != nil, wantStatus, wantErr, commit)
		}

		if next != nil {
			data = next
			m.check(t, data)
			if result.Revision != m.revision {
				t.Fatal("result revision does not match committed model")
			}

			if result.Status == Granted {
				want := m.records[r.ID]
				if result.Weight != want.request.Weight || result.Remaining != want.until.Sub(now) || result.LeaseID == "" {
					t.Fatalf("incorrect granted result: %+v", result)
				}
			}
		}
	}
}

func TestTransitionModelTraces(t *testing.T) {
	seeds, steps := 8, 128
	if os.Getenv("AIRLOCK_STRESS") == "1" {
		// Budget 64*250 = 16,000 transitions versus 8*128 = 1,024 normally
		// (about 16x), preserving seed diversity and both priority modes while
		// bounding race-instrumented JSON round trips. This serial model has no
		// worker interleavings; the stress runner executes it once, outside the
		// CPU repetition matrix used by concurrent Local and Shared tests.
		// Measured once with -race: 15.8s on Go 1.27.1/darwin arm64 (M3 Pro);
		// this is a tuning reference, not a wall-time assertion or CI SLA.
		seeds, steps = 64, 250
	}

	for seed := 0; seed < seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(seed)))
			trace := make([]byte, steps)
			if _, err := rng.Read(trace); err != nil {
				t.Fatal(err)
			}

			runProtocolTrace(t, trace, seed%2 == 1)
		})
	}
}

func TestTransitionRestoredRequestFieldValidation(t *testing.T) {
	mutations := map[string]func(*Request){
		"owner":           func(r *Request) { r.Owner = "invalid" },
		"tenant NUL":      func(r *Request) { r.Tenant = "tenant\x00" },
		"tenant length":   func(r *Request) { r.Tenant = strings.Repeat("t", 257) },
		"ID deadline":     func(r *Request) { r.SubmitUntil = r.SubmitUntil.Add(time.Millisecond) },
		"fractional wait": func(r *Request) { r.WaitUntil = r.WaitUntil.Add(time.Nanosecond) },
		"future wait":     func(r *Request) { r.WaitUntil = r.SubmitUntil.Add(r.Config.MaxWait + time.Millisecond) },
		"negative weight": func(r *Request) { r.Weight = -1 },
		"zero weight":     func(r *Request) { r.Weight = 0 },
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			s, cmd := scaleSnapshot(t, 2, 64, 0)

			for id, rec := range s.Records {
				if id != cmd.Request.ID {
					mutate(&rec.Request)
				}
			}

			data, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}

			next, _, err := transitionSnapshot(data, s.Time, cmd)
			if next != nil || !errors.Is(err, ErrCorruptState) {
				t.Fatalf("invalid restored fields accepted: next=%t error=%v", next != nil, err)
			}
		})
	}
}

func FuzzTransitionSnapshot(f *testing.F) {
	baseline, now, cmd := scaleBytes(f, 4, 0, 2)
	f.Add(baseline)
	f.Add([]byte("null"))
	f.Add([]byte("{"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, input []byte) {
		// Bounded corruption exploration, not a guarantee against arbitrarily
		// large hostile JSON inputs or an external backend access-control test.
		if len(input) > 16*1024 {
			t.Skip()
		}

		original := append([]byte(nil), input...)
		next, result, err := transitionSnapshot(input, now, cmd)
		if !bytes.Equal(input, original) {
			t.Fatal("transition mutated caller's snapshot")
		}

		again, res, e := transitionSnapshot(input, now, cmd)
		if !bytes.Equal(next, again) || res != result || !errors.Is(e, err) {
			t.Fatal("same snapshot/command/time was nondeterministic")
		}
		if next == nil {
			if err == nil {
				t.Fatal("nil snapshot with no error")
			}
			return
		}
		if _, _, e := transitionSnapshot(next, now, cmd); errors.Is(e, ErrCorruptState) || errors.Is(e, ErrConfigConflict) {
			t.Fatalf("committed snapshot cannot be restored: %v", e)
		}

		var s durableSnapshot
		if e := json.Unmarshal(next, &s); e != nil {
			t.Fatal(e)
		}

		used := 0

		for _, rec := range s.Records {
			if rec.Status == Granted {
				used += rec.Request.Weight
			}
		}

		if used < 0 || used > s.Config.Admission.Capacity || len(s.Records) > s.Config.MaxRecords {
			t.Fatalf("accepted snapshot violates bounds: used=%d records=%d", used, len(s.Records))
		}
	})
}

func FuzzTransitionCommandSequence(f *testing.F) {
	f.Add([]byte{0, 16, 32, 48, 8, 9, 10, 11, 12, 13, 255}, false)
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 254, 253}, true)
	f.Fuzz(func(t *testing.T, trace []byte, priority bool) {
		if len(trace) > 128 {
			t.Skip()
		}

		runProtocolTrace(t, trace, priority)
	})
}

func checkOptimisticWindow(t testing.TB, offset, span uint16, queued bool) {
	t.Helper()
	q := 0
	if queued {
		q = 3
	}

	s, cmd := scaleSnapshot(t, 8, 4, q)
	deadline := s.Time.Add(time.Duration(2+int(span)%31) * time.Millisecond)
	cmd.Request.WaitUntil = deadline

	for id, rec := range s.Records {
		// Vary both command-only and persisted deadlines. Observe cannot change
		// admission within the window: a holder is either absent or still live.
		rec.Request.WaitUntil = deadline.Add(time.Duration(len(id)%3) * time.Millisecond)
		if id == cmd.Request.ID {
			rec.Request.WaitUntil = cmd.Request.WaitUntil
		}

		if rec.Status == Granted || rec.Status == Queued {
			rec.Until = rec.Request.WaitUntil
		}

		rec.Request.Config = ResourceConfig{}
	}

	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}

	now := s.Time.Add(time.Duration(offset%1000) * time.Microsecond)
	next, result, window, err := equivalentWindowTransition(t, data, now, cmd)
	plain, reference, referenceErr := transitionSnapshot(data, now, cmd)
	if err != nil || referenceErr != nil || !bytes.Equal(next, plain) || result != reference {
		t.Fatalf("optimistic differs from transition: %v/%v", err, referenceErr)
	}
	// The observed request's WaitUntil is the earliest boundary in this fixture;
	// all retained GC/submit and other active deadlines are at or after it.
	if window.NotBefore != now || !window.Before.Equal(deadline) || !window.Allows(now) ||
		window.Allows(now.Add(-time.Nanosecond)) || window.Allows(deadline) || !window.Allows(deadline.Add(-time.Nanosecond)) {
		t.Fatalf("incorrect publication bounds: %+v want [%v,%v)", window, now, deadline)
	}

	later := now.Add(deadline.Sub(now) / 2)
	laterData, laterResult, _, err := transitionOptimistic(data, later, cmd)
	if err != nil || laterResult.Status != result.Status {
		t.Fatalf("within-window observation changed lifecycle: %+v/%+v %v", result, laterResult, err)
	}

	var before, after durableSnapshot
	if err := json.Unmarshal(next, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(laterData, &after); err != nil {
		t.Fatal(err)
	}

	after.Time = before.Time
	if !reflect.DeepEqual(before, after) {
		t.Fatal("within-window Observe changed records or scheduling state")
	}
	if result.Status == Granted && laterResult.Remaining != result.Remaining-later.Sub(now) {
		t.Fatal("remaining lease time did not subtract publication delay")
	}
}

func TestOptimisticWindowProperties(t *testing.T) {
	for _, offset := range []uint16{0, 1, 999} {
		for _, span := range []uint16{0, 1, 30, 65535} {
			checkOptimisticWindow(t, offset, span, false)
			checkOptimisticWindow(t, offset, span, true)
		}
	}
}

func FuzzOptimisticWindow(f *testing.F) {
	f.Add(uint16(0), uint16(0), false)
	f.Add(uint16(999), uint16(30), true)
	f.Fuzz(func(t *testing.T, offset, span uint16, queued bool) {
		checkOptimisticWindow(t, offset, span, queued)
	})
}

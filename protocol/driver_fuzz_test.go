package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestApplySnapshotRestoredRequestFieldValidation(t *testing.T) {
	mutations := map[string]func(*Request){
		"owner":            func(r *Request) { r.Owner = "invalid" },
		"tenant NUL":       func(r *Request) { r.Tenant = "tenant\x00" },
		"tenant length":    func(r *Request) { r.Tenant = strings.Repeat("t", 257) },
		"ID deadline":      func(r *Request) { r.SubmitUntil = r.SubmitUntil.Add(time.Millisecond) },
		"fractional wait":  func(r *Request) { r.WaitUntil = r.WaitUntil.Add(time.Nanosecond) },
		"future wait":      func(r *Request) { r.WaitUntil = r.SubmitUntil.Add(r.Config.MaxWait + time.Millisecond) },
		"negative weight":  func(r *Request) { r.Weight = -1 },
		"excessive weight": func(r *Request) { r.Weight = r.Config.Admission.Capacity + 1 },
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
			next, _, err := ApplySnapshot(data, s.Time, cmd)
			if next != nil || !errors.Is(err, ErrCorruptState) {
				t.Fatalf("invalid restored fields accepted: next=%t error=%v", next != nil, err)
			}
		})
	}
}

func FuzzSnapshotDecode(f *testing.F) {
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
		next, result, err := ApplySnapshot(input, now, cmd)
		if !bytes.Equal(input, original) {
			t.Fatal("transition mutated caller's snapshot")
		}
		again, res, e := ApplySnapshot(input, now, cmd)
		if !bytes.Equal(next, again) || res != result || !errors.Is(e, err) {
			t.Fatal("same snapshot/command/time was nondeterministic")
		}
		if next == nil {
			if err == nil {
				t.Fatal("nil snapshot with no error")
			}
			return
		}
		if _, _, e := ApplySnapshot(next, now, cmd); errors.Is(e, ErrCorruptState) || errors.Is(e, ErrConfigConflict) {
			t.Fatalf("committed snapshot cannot be restored: %v", e)
		}
		var s State
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

func FuzzApplySnapshotCommandSequence(f *testing.F) {
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
	next, result, window, err := ApplyOptimistic(data, now, cmd)
	plain, reference, referenceErr := ApplySnapshot(data, now, cmd)
	if err != nil || referenceErr != nil || !bytes.Equal(next, plain) || result != reference {
		t.Fatalf("optimistic differs from transition: %v/%v", err, referenceErr)
	}
	// The observed request's WaitUntil is the earliest boundary in this fixture;
	// all retained GC/submit and other active deadlines are at or after it.
	if window.NotBefore != now || !window.Before.Equal(deadline) || !window.Allows(now) || window.Allows(now.Add(-time.Nanosecond)) || window.Allows(deadline) || !window.Allows(deadline.Add(-time.Nanosecond)) {
		t.Fatalf("incorrect publication bounds: %+v want [%v,%v)", window, now, deadline)
	}
	later := now.Add(deadline.Sub(now) / 2)
	laterData, laterResult, _, err := ApplyOptimistic(data, later, cmd)
	if err != nil || laterResult.Status != result.Status {
		t.Fatalf("within-window observation changed lifecycle: %+v/%+v %v", result, laterResult, err)
	}
	var before, after State
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

package airlock

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

func copyPartialState(t testing.TB, s *SharedState) *SharedState {
	t.Helper()
	data, err := EncodeState(s)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeState(data)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// partitionForTest models an authoritative expiry index. Due non-target records
// may already have been physically reclaimed; future omitted records are counted.
func partitionForTest(t testing.TB, s *SharedState, now time.Time, cmd Command) (*SharedState, RetainedHistory, map[string]*DriverRecord) {
	t.Helper()
	part := copyPartialState(t, s)
	history := RetainedHistory{}
	untouched := make(map[string]*DriverRecord)
	for id, rec := range part.Records {
		if rec.Status == Granted || rec.Status == Queued || (cmd.Kind != Resize && id == cmd.Request.ID) {
			continue
		}
		at := rec.Finished.Add(part.Config.Retention)
		if rec.Request.SubmitUntil.After(at) {
			at = rec.Request.SubmitUntil
		}
		delete(part.Records, id)
		if !at.After(now.Truncate(time.Millisecond)) {
			continue
		}
		history.Count++
		if history.Before.IsZero() || at.Before(history.Before) {
			history.Before = at
		}
		untouched[id] = rec
	}
	return part, history, untouched
}

func TestTransitionPartialMatchesCompleteState(t *testing.T) {
	for _, retention := range []time.Duration{30 * time.Minute, time.Hour, 2 * time.Hour} {
		for _, offset := range []time.Duration{0, time.Nanosecond, time.Hour - time.Nanosecond, time.Hour, time.Hour + time.Nanosecond, 2 * time.Hour} {
			s, _ := scaleSnapshot(t, 12, 0, 3)
			s.Config.Retention = retention
			// A full budget makes removal of due omitted records observable to admission.
			s.Config.MaxRecords = len(s.Records)
			ids := make([]string, 0, len(s.Records))
			for id, r := range s.Records {
				r.Request.Config = s.Config
				ids = append(ids, id)
			}
			sort.Strings(ids)
			now := s.Time.Add(offset)
			unknown := stateTestRequest(now, s.Config, "new", 1)
			unknown.Resource = s.Resource
			requests := []Request{unknown}
			for _, id := range ids {
				requests = append(requests, s.Records[id].Request)
			}
			var commands []Command
			for _, r := range requests {
				for _, kind := range []Operation{Submit, Observe, Cancel, Renew, Release} {
					commands = append(commands, Command{Kind: kind, Request: r, LeaseID: leaseIdentity(r)})
				}
			}
			for _, capacity := range []int{0, 1, 2} {
				commands = append(commands, Command{Kind: Resize, Resize: ResizeRequest{Resource: s.Resource, Capacity: capacity, ExpectedVersion: s.CapacityVersion}})
			}
			for i, cmd := range commands {
				t.Run(fmt.Sprintf("retention=%s/offset=%s/cmd=%d", retention, offset, i), func(t *testing.T) {
					full, res, w, err := Transition(copyPartialState(t, &s), now, cmd)
					part, h, untouched := partitionForTest(t, &s, now, cmd)
					got, result, window, e := TransitionPartial(part, now, cmd, h)
					if !errors.Is(e, err) || !reflect.DeepEqual(result, res) || !reflect.DeepEqual(window, w) {
						t.Fatalf("partial=(%+v,%+v,%v), full=(%+v,%+v,%v)", result, window, e, res, w, err)
					}
					if got != nil {
						for id, r := range untouched {
							r.Request.Config = ResourceConfig{}
							got.Records[id] = r
						}
					}
					if !reflect.DeepEqual(got, full) {
						t.Fatalf("recombined partial state differs from full transition")
					}
				})
			}
		}
	}
}

func TestTransitionPartialDueTargetConflictBeforeGC(t *testing.T) {
	s, cmd := scaleSnapshot(t, 3, 0, 0)
	now := s.Time.Add(time.Hour)
	cmd.Request.Owner = NewRequest(cmd.Request.SubmitUntil).Owner
	part, h, _ := partitionForTest(t, &s, now, cmd)
	if h.Count != 0 || len(part.Records) != 1 {
		t.Fatal("fixture did not retain only due target")
	}
	next, _, window, err := TransitionPartial(part, now, cmd, h)
	if !errors.Is(err, ErrRequestConflict) || next != nil || window != (CommitWindow{}) {
		t.Fatalf("%+v %v", next, err)
	}
}

func TestTransitionPartialMetadataValidation(t *testing.T) {
	s, cmd := scaleSnapshot(t, 3, 0, 0)
	now := s.Time
	cases := []struct {
		name    string
		state   *SharedState
		history RetainedHistory
	}{
		{"negative", &s, RetainedHistory{Count: -1}},
		{"unexpected boundary", &s, RetainedHistory{Before: now.Add(time.Hour)}},
		{"missing boundary", &s, RetainedHistory{Count: 1}},
		{"past boundary", &s, RetainedHistory{Count: 1, Before: now.Add(-time.Nanosecond)}},
		{"equal boundary", &s, RetainedHistory{Count: 1, Before: now}},
		{"new resource", nil, RetainedHistory{Count: 1, Before: now.Add(time.Hour)}},
		{"overflow", &s, RetainedHistory{Count: int(^uint(0) >> 1), Before: now.Add(time.Hour)}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			state := tt.state
			if state != nil {
				state = copyPartialState(t, state)
			}
			next, _, w, err := TransitionPartial(state, now, cmd, tt.history)
			if next != nil || w != (CommitWindow{}) || !errors.Is(err, ErrCorruptState) {
				t.Fatalf("got %v %v", next, err)
			}
		})
	}
	r := stateTestRequest(now, stateTestConfig(), "a", 1)
	next, _, _, err := TransitionPartial(nil, now, Command{Kind: Submit, Request: r}, RetainedHistory{})
	if err != nil || next == nil {
		t.Fatal(err)
	}
}

func BenchmarkTransitionPartialHistory(b *testing.B) {
	for _, records := range []int{16, 10000} {
		b.Run(fmt.Sprint(records), func(b *testing.B) {
			s, cmd := scaleSnapshot(b, records, 0, 3)
			part, h, _ := partitionForTest(b, &s, s.Time, cmd)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				next, _, _, err := TransitionPartial(copyPartialState(b, part), s.Time, cmd, h)
				if err != nil || next == nil {
					b.Fatal(err)
				}
			}
		})
	}
}

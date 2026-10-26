package airlock

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

// scaleSnapshot constructs a valid snapshot in O(records+mappings), rather than
// warming it with records sequential transitions (which would hide setup cost).
// Records includes one holder when queued > 0; the remainder are terminal.
func scaleSnapshot(tb testing.TB, records, mappings, queued int) (durableSnapshot, Command) {
	tb.Helper()
	if records < 1 || queued < 0 || queued >= records {
		tb.Fatal("invalid scale fixture dimensions")
	}
	now := time.Unix(2000, 0).UTC()
	cfg := ResourceConfig{Admission: Config{Capacity: 1, MaxQueued: 10000, MaxQueuedPerTenant: 10000},
		LeaseTTL: time.Hour, MaxWait: time.Hour, SubmitWindow: time.Hour, Retention: time.Hour, MaxRecords: 10000}
	if mappings > 0 {
		p := &PriorityPolicy{Groups: []PriorityGroup{{Name: "all", Turns: 1, MaxQueued: 10000}}, DefaultGroup: "all", TenantGroups: make(map[string]string, mappings)}
		for i := 0; i < mappings; i++ {
			p.TenantGroups[fmt.Sprintf("tenant-%05d", i)] = "all"
		}
		cfg.Admission.Priority = p
	}
	s := durableSnapshot{Version: 1, Resource: "scale", Config: cfg, Records: make(map[string]*durableRecord, records), Queues: make(map[string][]string), Revision: 1, Time: now, Rotations: [][]string{{}}}
	var cmd Command
	for i := 0; i < records; i++ {
		r := Request{Resource: s.Resource, ID: fmt.Sprintf("%d-%032x", now.Add(time.Hour).UnixMilli(), i+1), Owner: fmt.Sprintf("%032x", i+1), SubmitUntil: now.Add(time.Hour), WaitUntil: now.Add(time.Hour), Weight: 1, Config: cfg}
		rec := &durableRecord{Request: r, Status: Cancelled, Finished: now}
		if queued > 0 && i == 0 {
			rec.Status, rec.Finished, rec.Until = Granted, time.Time{}, now.Add(cfg.LeaseTTL)
			rec.LeaseID = leaseIdentity(r)
		} else if i > 0 && i <= queued {
			rec.Request.Tenant = "waiting"
			rec.Status, rec.Finished, rec.Until = Queued, time.Time{}, r.WaitUntil
			s.Queues["waiting"] = append(s.Queues["waiting"], r.ID)
		}
		s.Records[r.ID] = rec
		if i == 0 {
			cmd = Command{Kind: Observe, Request: r}
		}
	}
	if queued > 0 {
		s.Rotations[0] = []string{"waiting"}
	}
	return s, cmd
}

func scaleBytes(tb testing.TB, records, mappings, queued int) ([]byte, time.Time, Command) {
	tb.Helper()
	s, cmd := scaleSnapshot(tb, records, mappings, queued)
	for _, r := range s.Records {
		r.Request.Config = ResourceConfig{}
	}
	data, err := json.Marshal(s)
	if err != nil {
		tb.Fatal(err)
	}
	if next, _, err := transitionSnapshot(data, s.Time, cmd); err != nil || next == nil {
		tb.Fatalf("invalid scale fixture: %v", err)
	}
	return data, s.Time, cmd
}

func TestRestorePriorityAllocationScaling(t *testing.T) {
	// Isolate restoration from JSON caches, codecs, hash-key sorting and fixture
	// construction. AllocsPerRun performs a warm-up; every call gets the same
	// immutable input. Compare the incremental priority overhead, not absolute
	// allocation counts (which vary across Go versions and architectures).
	allocations := func(records, mappings int) float64 {
		s, _ := scaleSnapshot(t, records, mappings, 0)
		return testing.AllocsPerRun(3, func() {
			if _, _, err := s.restore(); err != nil {
				t.Fatal(err)
			}
		})
	}
	smallBase, largeBase := allocations(8, 0), allocations(128, 0)
	smallPriority, largePriority := allocations(8, 1024), allocations(128, 1024)
	smallExtra, largeExtra := smallPriority-smallBase, largePriority-largeBase
	t.Logf("restore allocations: R8 plain=%.0f priority=%.0f; R128 plain=%.0f priority=%.0f; incremental=%.0f/%.0f", smallBase, smallPriority, largeBase, largePriority, smallExtra, largeExtra)
	// A single compiled policy has near-constant overhead. Allow a wide margin
	// for map implementations; compiling once per record grows by 16x here.
	if largeExtra > 3*smallExtra+64 {
		t.Fatalf("priority compilation scales with records: extra allocations %.0f -> %.0f", smallExtra, largeExtra)
	}
}

func TestTransitionScaleFixtures(t *testing.T) {
	for _, records := range []int{1, 8, 128} {
		for _, mappings := range []int{0, 64} {
			for _, queued := range []int{0, records - 1} {
				data, now, cmd := scaleBytes(t, records, mappings, queued)
				next, result, err := transitionSnapshot(data, now, cmd)
				if err != nil || next == nil || result.Revision != 2 {
					t.Fatalf("R=%d M=%d Q=%d: %+v %v", records, mappings, queued, result, err)
				}
			}
		}
	}
}

// The default matrix stays small. Opt in to deployment-limit dimensions with
// AIRLOCK_BENCH_LARGE=1 and select individual R/M/Q subbenchmarks as needed.
// Fixtures and their validation are excluded from timing; each operation uses
// the identical snapshot/backend time, so history cannot grow with b.N.
func benchmarkTransitionScale(b *testing.B, optimistic bool) {
	recordCounts, mappingCounts := []int{8, 128}, []int{0, 64}
	if os.Getenv("AIRLOCK_BENCH_LARGE") == "1" {
		recordCounts = append(recordCounts, 1000, 10000)
		mappingCounts = append(mappingCounts, 1000, 10000)
	}
	for _, records := range recordCounts {
		for _, mappings := range mappingCounts {
			for _, queued := range []int{0, records - 1} {
				b.Run(fmt.Sprintf("R=%d/M=%d/Q=%d", records, mappings, queued), func(b *testing.B) {
					data, now, cmd := scaleBytes(b, records, mappings, queued)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var next []byte
						var result Result
						var err error
						if optimistic {
							next, result, _, err = transitionOptimistic(data, now, cmd)
						} else {
							next, result, err = transitionSnapshot(data, now, cmd)
						}
						if err != nil || len(next) == 0 || result.Revision != 2 {
							b.Fatalf("invalid transition: %+v %v", result, err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(len(data)), "snapshot_bytes")
				})
			}
		}
	}
}

func BenchmarkTransitionScale(b *testing.B) {
	benchmarkTransitionScale(b, false)
}

func BenchmarkTransitionOptimisticScale(b *testing.B) {
	benchmarkTransitionScale(b, true)
}

package main

import (
	a "github.com/tr1v3r/airlock"
	"testing"
	"time"
)

func TestFixtureLifecycleAndRetention(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	c := config{History: 100, Capacity: 8, Workers: 32, Scenario: "steady"}
	cfg := a.ResourceConfig{Admission: a.Config{Capacity: 8, MaxQueued: 64, MaxQueuedPerTenant: 64}, LeaseTTL: ttl, MaxWait: 10 * time.Second, SubmitWindow: 5 * time.Second, Retention: time.Minute, MaxRecords: 10000}
	for _, mode := range []string{"steady", "expiry-gc"} {
		c.Scenario = mode
		s, err := fixture(now, c, cfg, "r")
		if err != nil {
			t.Fatal(err)
		}
		want := 100
		if mode == "expiry-gc" {
			want += 16
		}
		if len(s.Records) != want {
			t.Fatal(len(s.Records))
		}
		s, _, _, err = a.Transition(s, now.Add(3*time.Second), a.Command{Kind: a.Resize, Resize: a.ResizeRequest{Resource: "r", Capacity: 8, ExpectedVersion: 1}})
		if err != nil {
			t.Fatal(err)
		}
		if mode == "steady" && len(s.Records) != 100 {
			t.Fatal("lost retained history")
		}
		if mode == "expiry-gc" {
			if len(s.Records) != 16 {
				t.Fatal("history did not GC")
			}
			for _, rec := range s.Records {
				if rec.Status != a.Expired {
					t.Fatal(rec.Status)
				}
			}
		}
	}
}
func TestSummarySeparatesBoundaryAndCleanup(t *testing.T) {
	c := config{Duration: time.Second}
	data := []sample{{Op: "submit", Phase: "measured", Status: "granted", LatencyMS: 1}, {Op: "observe", Phase: "measured", Error: "deadline", ErrorClass: "deadline", LatencyMS: 2}, {Op: "renew", Phase: "boundary", Error: "deadline"}, {Op: "cancel", Phase: "cleanup", Error: "deadline"}}
	r := summarize(c, data)
	if r.Operations != 2 || r.Successes != 1 || r.BoundaryOperations != 1 || r.BoundaryErrors != 1 || r.CleanupErrors != 1 || r.OperationsPerSecond != 2 {
		t.Fatalf("%+v", r)
	}
	if r.Latencies["submit/ok"].P99 != nil {
		t.Fatal("invented p99 from one sample")
	}
}
func TestDistributionSampleGates(t *testing.T) {
	for _, n := range []int{0, 1, 19, 20, 99, 100} {
		v := make([]float64, n)
		for i := range v {
			v[i] = float64(i + 1)
		}
		d := describe(v)
		if d.N != n || (d.P95 != nil) != (n >= 20) || (d.P99 != nil) != (n >= 100) {
			t.Fatal(n, d)
		}
	}
}

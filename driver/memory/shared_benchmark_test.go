package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

// Black-box comparison counterpart of the root-internal Shared harness
// (shared_benchmark_test.go in the root module): a real memory.Driver plus
// airlock.NewShared over the W matrix. H stays 0 because memory.Driver starts
// empty and offers no snapshot seeding; the root harness carries the H
// dimension. Protocol parameters mirror the root harness exactly (same
// LeaseTTL/Retention/PollInterval/MaxRecords and the same per-point window
// derivation, duplicated below) so cycles_per_s and latency percentiles are
// interpretable between the internal benchDriver and this reference driver.
// Both use cached Observation reads, but root skips the resource lookup mutex
// and this module may pin a different core revision (inspect go list -m all).
// Equal timings are not an invariant. The
// worker pattern is likewise duplicated: root test files are not importable.
//
// Benchtime guidance, as in the root harness: contended cells reach steady
// queue depth only after roughly 4*W cycles, so use -benchtime=Nx with
// N >= 4*W there (for example -benchtime=1200x at W=256).

const (
	// memoryBenchContendedCapacity mirrors the root harness's fixed pool.
	memoryBenchContendedCapacity = 8

	// Fixed protocol timing, identical to the root harness's constants; see
	// the rationale there. LeaseTTL exceeds hold and the worst
	// grant-to-observation delay, Retention is the shortest legal value so
	// runtime terminals reclaim once SubmitUntil passes, and PollInterval is
	// 2ms because the 50ms default would dominate queued throughput.
	memoryBenchLeaseTTL     = 30 * time.Second
	memoryBenchRetention    = time.Millisecond
	memoryBenchPollInterval = 2 * time.Millisecond

	// Window bounds, mirroring the root harness's floor and ceiling.
	memoryBenchSubmitWindowFloor = 10 * time.Millisecond
	memoryBenchSubmitWindowCeil  = 35 * time.Second

	// memoryBenchMaxWaitSkipThreshold mirrors the root harness's guard: cells
	// whose derived contended MaxWait would exceed it are skipped loudly. With
	// H=0 the W matrix never reaches it (worst case, W=256, derives ~4.6 minutes).
	memoryBenchMaxWaitSkipThreshold = 55 * time.Minute
)

// memoryBenchTransitionEstimate mirrors the root harness's calibration
// (BenchmarkTransitionScale, darwin/arm64 go1.27, codec included after the
// SharedState unification): one serialized transition costs ~86us fixed plus
// ~3.9us per retained record, rounded up so faster machines stay inside the
// derived margins.
func memoryBenchTransitionEstimate(records int) time.Duration {
	return 100*time.Microsecond + 4200*time.Nanosecond*time.Duration(records)
}

// memoryBenchCeilMillis rounds d up to a whole millisecond: the protocol
// rejects sub-millisecond durations, and rounding down could shave the safety
// factor.
func memoryBenchCeilMillis(d time.Duration) time.Duration {
	if rem := d % time.Millisecond; rem != 0 {
		return d + time.Millisecond - rem
	}
	return d
}

// memoryBenchWindows uses the same conservative safety budgets as the root
// harness. SubmitWindow covers a backlog of full transitions; contended MaxWait
// retains the historical W-squared allowance even though ordinary Observe now
// reads cached metadata. These are deadline budgets, not latency predictions.
// The module's pinned core may differ from the working root; report both versions.
func memoryBenchWindows(workers int, contended bool) (submitWindow, maxWait, operationTimeout time.Duration) {
	gate := time.Duration(workers) * memoryBenchTransitionEstimate(3*workers)
	submitWindow = memoryBenchCeilMillis(max(memoryBenchSubmitWindowFloor, min(4*gate, memoryBenchSubmitWindowCeil)))
	maxWait = max(30*time.Second, 2*submitWindow)
	if contended {
		// 1.25 * W^2 * tc == 5 * W * gate / 4, in exact rational arithmetic.
		maxWait = max(maxWait, memoryBenchCeilMillis(5*gate*time.Duration(workers)/4))
	}
	operationTimeout = min(time.Minute, submitWindow+5*time.Second)
	return submitWindow, maxWait, operationTimeout
}

// memoryBenchConfig builds the resource policy for one matrix point. Workers
// get one distinct tenant each, so MaxQueuedPerTenant=2 leaves slack above the
// single outstanding request a worker can have.
func memoryBenchConfig(workers, capacity, maxQueued int) airlock.ResourceConfig {
	submitWindow, maxWait, _ := memoryBenchWindows(workers, maxQueued > 0)
	return airlock.ResourceConfig{
		Admission: airlock.Config{Capacity: capacity, MaxQueued: maxQueued,
			MaxQueuedPerTenant: min(2, maxQueued)},
		LeaseTTL:     memoryBenchLeaseTTL,
		MaxWait:      maxWait,
		SubmitWindow: submitWindow,
		Retention:    memoryBenchRetention,
		MaxRecords:   10000,
	}
}

// memoryBenchCycles runs the root harness's worker pattern: W long-lived
// goroutines claim cycles from one atomic counter, self-time each Acquire,
// release immediately, and record into one sample array preallocated before
// the timer starts (the claim number is the slot index, so the hot path never
// allocates and no merge pass is needed; a worker skips its slot only on a
// failure, and any failure is Fatal before the return). Errors travel over a
// channel and Fatal only in the calling goroutine; the first failure aborts
// siblings by cancelling the run context. Worker startup and the join stay
// outside the timed region; the hot path never touches the timer. It returns
// the completed-cycle count and the merged, sorted latencies.
func memoryBenchCycles(tb testing.TB, client *airlock.Shared, workers int, cycles int64) (int64, []time.Duration) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var cursor atomic.Int64
	var abort atomic.Bool
	failures := make(chan error, workers)
	samples := make([]time.Duration, cycles)
	var ready, finished sync.WaitGroup
	start := make(chan struct{})
	fail := func(slot int, operation string, err error) {
		abort.Store(true)
		stop()
		select {
		case failures <- fmt.Errorf("worker %d: %s: %w", slot, operation, err):
		default:
		}
	}
	for slot := range workers {
		ready.Add(1)
		finished.Add(1)
		go func(slot int) {
			defer finished.Done()
			tenant := fmt.Sprintf("bench-worker-%04d", slot)
			ready.Done()
			<-start
			for !abort.Load() {
				seq := cursor.Add(1)
				if seq > cycles {
					return
				}
				cycleStart := time.Now()
				lease, err := client.AcquireFor(ctx, tenant)
				if err != nil {
					fail(slot, "acquire", err)
					return
				}
				// Acquire latency only: sample before the release round trip.
				waited := time.Since(cycleStart)
				if err = lease.Release(context.Background()); err != nil {
					fail(slot, "release", err)
					return
				}
				samples[seq-1] = waited
			}
		}(slot)
	}
	ready.Wait()
	if bench, ok := tb.(*testing.B); ok {
		bench.ResetTimer()
	}
	close(start)
	finished.Wait()
	if bench, ok := tb.(*testing.B); ok {
		bench.StopTimer()
	}
	close(failures)
	for err := range failures {
		tb.Fatal(err)
	}
	slices.Sort(samples)
	return int64(len(samples)), samples
}

// memoryBenchPercentile uses the nearest-rank method on sorted samples: rank
// is ceil(q*N), clamped to [1,N], and the result is sorted[rank-1].
func memoryBenchPercentile(sorted []time.Duration, q float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := int(math.Ceil(q * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

// memoryBenchSteadyRecords counts records in the driver's committed snapshot.
// The join happens-before this read, so unlocked access is race-free here.
func memoryBenchSteadyRecords(tb testing.TB, d *Driver, resource string) int {
	tb.Helper()
	r := d.resources[resource]
	if r == nil || len(r.snapshot) == 0 {
		return 0
	}
	var snapshot struct {
		Records map[string]json.RawMessage
	}
	if err := json.Unmarshal(r.snapshot, &snapshot); err != nil {
		tb.Fatal(err)
	}
	return len(snapshot.Records)
}

// benchmarkMemoryShared runs the W matrix for one family. Uncontended sets
// Capacity=W (a worker submits only while holding nothing, so peers hold at
// most W-1 tokens and Submit is always granted) and MaxQueued=0 so accidental
// queuing fails loudly. Contended pins Capacity=8 and queues the rest.
func benchmarkMemoryShared(b *testing.B, contended bool) {
	for _, workers := range []int{1, 4, 16, 64, 256} {
		b.Run(fmt.Sprintf("W=%d", workers), func(b *testing.B) {
			resource, capacity, maxQueued := "memory-shared-bench", workers, 0
			if contended {
				resource, capacity, maxQueued = "memory-shared-bench-contended",
					memoryBenchContendedCapacity, workers+16
			}
			_, maxWait, operationTimeout := memoryBenchWindows(workers, contended)
			if maxWait > memoryBenchMaxWaitSkipThreshold {
				b.Skipf("W=%d: derived MaxWait %s exceeds %s; steady-state drain is unmeasurable within protocol limits",
					workers, maxWait, memoryBenchMaxWaitSkipThreshold)
			}
			d := New()
			client, err := airlock.NewShared(d, airlock.SharedOptions{Resource: resource,
				Config:           memoryBenchConfig(workers, capacity, maxQueued),
				OperationTimeout: operationTimeout, PollInterval: memoryBenchPollInterval})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			completed, latencies := memoryBenchCycles(b, client, workers, int64(b.N))
			if seconds := b.Elapsed().Seconds(); seconds > 0 {
				b.ReportMetric(float64(completed)/seconds, "cycles_per_s")
			}
			for _, q := range []struct {
				fraction float64
				key      string
			}{{0.50, "latency_p50_ns"}, {0.95, "latency_p95_ns"}, {0.99, "latency_p99_ns"}, {0.999, "latency_p999_ns"}} {
				b.ReportMetric(float64(memoryBenchPercentile(latencies, q.fraction)), q.key)
			}
			if n := len(latencies); n > 0 {
				b.ReportMetric(float64(latencies[n-1]), "latency_max_ns")
			}
			b.ReportMetric(float64(memoryBenchSteadyRecords(b, d, resource)), "steady_records")
			b.ReportMetric(float64(capacity), "effective_capacity")
		})
	}
}

// BenchmarkMemorySharedUncontended measures Submit-is-Granted throughput and
// Acquire latency with capacity for every worker (no queuing, no polling).
func BenchmarkMemorySharedUncontended(b *testing.B) { benchmarkMemoryShared(b, false) }

// BenchmarkMemorySharedContended measures queued acquisition under a fixed
// capacity of 8 through the real reference driver.
func BenchmarkMemorySharedContended(b *testing.B) { benchmarkMemoryShared(b, true) }

// TestMemorySharedBenchmarkSmoke runs a few cycles for the module test gate
// (and the race detector) so the comparison file is exercised without -bench.
func TestMemorySharedBenchmarkSmoke(t *testing.T) {
	for _, p := range []struct {
		workers, cycles, capacity, maxQueued int
	}{
		{workers: 4, cycles: 64, capacity: 4, maxQueued: 0},
		{workers: 4, cycles: 64, capacity: 2, maxQueued: 20},
	} {
		cfg := memoryBenchConfig(p.workers, p.capacity, p.maxQueued)
		operationTimeout := memoryBenchTestWindows(&cfg, p.workers)
		client, err := airlock.NewShared(New(), airlock.SharedOptions{Resource: "memory-smoke",
			Config:           cfg,
			OperationTimeout: operationTimeout, PollInterval: memoryBenchPollInterval})
		if err != nil {
			t.Fatal(err)
		}
		completed, _ := memoryBenchCycles(t, client, p.workers, int64(p.cycles))
		if completed != int64(p.cycles) {
			t.Fatalf("completed %d of %d cycles", completed, p.cycles)
		}
	}
}

// memoryBenchTestWindows widens cfg's windows for test contexts and returns
// the matching OperationTimeout. The race lane runs the smoke test under
// -race, -cpu=1 and shuffled orders on shared CI VMs, where an instrumented
// transition costs an order of magnitude more than the calibration behind the
// 10ms SubmitWindow floor, and a Submit behind W-1 raced peers can miss it.
// Benchmarks keep the tight representative windows; tests assert liveness, so
// every window widens by 10x, still far inside the protocol caps (mirrors the
// root harness's rule).
func memoryBenchTestWindows(cfg *airlock.ResourceConfig, workers int) (operationTimeout time.Duration) {
	_, _, operationTimeout = memoryBenchWindows(workers, cfg.Admission.MaxQueued > 0)
	cfg.SubmitWindow *= 10
	cfg.MaxWait *= 10
	if operationTimeout *= 10; operationTimeout > time.Minute {
		operationTimeout = time.Minute
	}
	return operationTimeout
}

package airlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file benchmarks the Shared client layer (distributed.go) on top of a
// driver that mirrors driver/memory semantics: one serialization gate per
// resource plus the last committed encoded snapshot, decoding and re-encoding
// the structured state around every transition. Each driver operation is
// O(records) inside Transition plus codec, so the matrix varies W (long-lived
// workers, one tenant each) and H (retained seed records).
//
// Scope: every cycle is one single-token AcquireFor plus Release (weight 1),
// so cycles_per_s and the latency percentiles describe single-token admission
// only. Multi-token AcquireN, Renew and ClockSkewAllowance behavior is owned
// by the conformance and clock tests (distributed_*_test.go), not this matrix.
//
// Seed survival (the H dimension must persist for the whole run):
//   - Terminal seeds set SubmitUntil one hour out. Reclamation deletes a
//     terminal only once BOTH Finished+Retention and SubmitUntil have passed
//     (shared_state.go), so a far-future SubmitUntil keeps seeds alive through
//     every sweep even though the run itself uses the shortest Retention.
//   - No seed queued records: dispatch would grant them during unrelated
//     transitions and pollute the W dimension with phantom contenders.
//   - No seed granted holders ("few holders" taken to zero): configured
//     capacity stays fully effective (reported as effective_capacity, equal to
//     Capacity), avoiding compensation arithmetic. Live traffic already keeps
//     granted records in the snapshot, so restore still exercises that path.
//
// Runtime terminals (each cycle's released record) reclaim after their
// SubmitUntil passes plus the 1ms Retention; SubmitUntil is derived per matrix
// point (see sharedBenchWindows), and the steady_records metric shows how much
// history that horizon retains on top of H.
//
// Benchtime guidance: the contended family's queue depth builds toward W-8
// over the first cycles, so a deep-queue cell only reports steady state once
// roughly 4*W cycles have run. Use -benchtime=Nx with N >= 4*W there (for
// example -benchtime=1200x at W=256); plain -benchtime=1s truncates b.N inside
// the shallow-queue transient and reports inflated throughput. Contended
// cells whose derived MaxWait would exceed the protocol's 1h ceiling are
// skipped with a reason instead of producing invalid data; no cell in the
// current W x H matrix triggers that skip (worst case derives ~47 minutes).
//
// ErrCorruptState conclusion (audit B2, sporadic ~2-5% on darwin): the
// mechanism is the protocol's fail-closed backward-clock check. Transition
// compares each operation's backend time against the snapshot's last
// committed time after truncating both to whole milliseconds
// (shared_state.go: now.Before(s.Time)), a pure wall-clock comparison, so a
// client clock that steps backward by >= 1ms fails closed with
// ErrCorruptState; sub-millisecond steps are usually absorbed by truncation.
// TestSharedBenchBackwardClockFailsClosed proves this deterministically via
// the injectable driver clock. This is by design: benchDriver/memory.Driver
// read a local clock, while the postgres and redis drivers evaluate time
// after locking on the backend side and have no such exposure. The harness
// keeps the real clock by default for representativeness; matrix execution
// (task-3) handles the residual flake with per-cell retry and disclosure,
// and fail() dumps the snapshot/clock evidence whenever ErrCorruptState
// appears so future occurrences are immediately attributable.

// benchDriver mirrors driver/memory for one bench resource: a buffered
// channel of one serializes context-aware transitions, and the committed
// encoded snapshot survives between operations, paying the same decode and
// encode cost per transition as the reference driver. The initial snapshot is
// injectable so fixtures can pin the retained-record count H.
type benchDriver struct {
	// gate serializes transitions for the resource with context-aware waiting.
	gate chan struct{}
	// snapshot is the last committed encoded state; gate protects access.
	snapshot []byte
	// nowFunc supplies the backend evaluation time. The default time.Now
	// mirrors memory.Driver and keeps the harness representative of
	// clock-reading drivers; tests inject a stepped clock to prove the
	// protocol's fail-closed backward-clock behavior deterministically.
	nowFunc func() time.Time
}

// newBenchDriver prepares a driver over an optional committed seed snapshot.
func newBenchDriver(seed []byte) *benchDriver {
	return &benchDriver{gate: make(chan struct{}, 1), snapshot: seed, nowFunc: time.Now}
}

// execute serializes the resource and runs the shared durable state machine.
func (d *benchDriver) execute(ctx context.Context, cmd Command) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := ValidateCommand(cmd); err != nil {
		return Result{}, err
	}
	select {
	case d.gate <- struct{}{}:
		defer func() { <-d.gate }()
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	state, err := DecodeState(d.snapshot)
	if err != nil {
		return Result{}, err
	}
	next, result, _, err := Transition(state, d.nowFunc(), cmd)
	if next != nil {
		data, codecErr := EncodeState(next)
		if codecErr != nil {
			return Result{}, codecErr
		}
		d.snapshot = data
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return result, err
}

// Verify the bench driver implements the public Driver contract.
var _ Driver = (*benchDriver)(nil)

// Submit atomically submits the whole request without refreshing an existing lease.
func (d *benchDriver) Submit(ctx context.Context, request Request) (Result, error) {
	return d.execute(ctx, Command{Kind: Submit, Request: request})
}

// Observe returns the request outcome after advancing expiry and scheduling.
func (d *benchDriver) Observe(ctx context.Context, request Request) (Result, error) {
	return d.execute(ctx, Command{Kind: Observe, Request: request})
}

// Cancel cancels the request, including a grant or a cancellation before submission.
func (d *benchDriver) Cancel(ctx context.Context, request Request) (Result, error) {
	return d.execute(ctx, Command{Kind: Cancel, Request: request})
}

// Renew extends a live lease matching the original request and lease ID.
func (d *benchDriver) Renew(ctx context.Context, lease LeaseRef) (Result, error) {
	return d.execute(ctx, Command{Kind: Renew, Request: lease.Request, LeaseID: lease.ID})
}

// Release ends a matching lease and returns its whole weight at most once.
func (d *benchDriver) Release(ctx context.Context, lease LeaseRef) (Result, error) {
	return d.execute(ctx, Command{Kind: Release, Request: lease.Request, LeaseID: lease.ID})
}

const (
	// sharedBenchContendedCapacity pins the pool for the contended family.
	sharedBenchContendedCapacity = 8
	// sharedBenchMaxRecords is the protocol ceiling for retained records.
	sharedBenchMaxRecords = 10000
	// sharedBenchHeadroomBase and sharedBenchHeadroomPerWorker reserve slots
	// behind seed history for in-flight requests and terminals younger than
	// SubmitWindow, so Submit never hits ErrHistoryFull. At steady state a
	// terminal lives one SubmitWindow while the gate completes cycles at
	// roughly two transitions each, accumulating ~2W records (up to ~4W on a
	// machine twice as fast as the calibration below); 1024+6W bounds that
	// with margin. The clamp only ever bites at H=10000.
	sharedBenchHeadroomBase      = 1024
	sharedBenchHeadroomPerWorker = 6
	sharedBenchLeaseTTL          = 30 * time.Second
	sharedBenchRetention         = time.Millisecond
	sharedBenchPollInterval      = 2 * time.Millisecond
	sharedBenchSubmitWindowFloor = 10 * time.Millisecond
	sharedBenchSubmitWindowCeil  = 35 * time.Second
	// sharedBenchMaxWaitSkipThreshold guards the contended queue-depth MaxWait
	// derivation: cells whose derived MaxWait would exceed it (~55 minutes,
	// just under the 1h protocol cap) are skipped loudly instead of running
	// with a deadline that cannot cover steady-state draining. No cell in the
	// current matrix reaches it (the worst, W=256 at the largest clamped H,
	// derives ~47 minutes).
	sharedBenchMaxWaitSkipThreshold = 55 * time.Minute
)

// Protocol timing rationale (shared with driver/memory's comparison file so
// the two matrices are comparable):
//
//   LeaseTTL 30s  > hold: leases are released immediately after their latency
//                  sample, and no seed holders exist. 30s also covers the
//                  worst grant-to-observation delay (one gate backlog, ~W
//                  transitions) at every matrix point.
//   Retention 1ms: shortest legal retention, so runtime terminals are
//                  reclaimed as soon as their SubmitUntil passes. Terminal
//                  lifetime is dominated by SubmitWindow (see below).
//   PollInterval 2ms: the 50ms default would dominate queued throughput,
//                  because a waiter only learns about a dispatch-side grant
//                  on its next Observe.
//
// SubmitWindow, MaxWait and OperationTimeout are derived per matrix point by
// sharedBenchWindows; see its rationale.

// sharedBenchTransitionEstimate estimates one serialized transition at a given
// retained-record count, codec included. Calibrated with
// BenchmarkTransitionScale on darwin/arm64 go1.27 after the SharedState
// unification: ~86us fixed plus ~3.9us per record (R=128: 587us, R=1000:
// 3.57ms, R=10000: 39.2ms). The constants are rounded up so a modestly faster
// machine stays inside the derived margins.
func sharedBenchTransitionEstimate(records int) time.Duration {
	return 100*time.Microsecond + 4200*time.Nanosecond*time.Duration(records)
}

// benchCeilMillis rounds d up to a whole millisecond: the protocol rejects
// sub-millisecond durations, and rounding down could shave the safety factor.
func benchCeilMillis(d time.Duration) time.Duration {
	if rem := d % time.Millisecond; rem != 0 {
		return d + time.Millisecond - rem
	}
	return d
}

// sharedBenchWindows derives the protocol windows for one matrix point. The
// contended flag enables the queue-depth MaxWait model (see below).
//
// A Submit must first win the driver's serialization gate, which can hold up
// to W-1 peer operations (each worker keeps at most one outstanding driver
// call), each costing one O(records) transition. SubmitWindow therefore needs
// a multiple of W*transition(records): the 4x factor absorbs record
// accumulation and calibration drift, and the 10ms floor is the empirical
// minimum this harness needs (the protocol itself only requires a positive
// whole-millisecond window; below ~10ms even W=16/H=5000 expires
// submissions). The 35s ceiling keeps every derived value inside protocol
// bounds; it binds only at the W=256/H=10000 LARGE cell, where the cap still
// leaves ~4x of one gate backlog. SubmitWindow doubles as the retention horizon of runtime terminals:
// a bigger window retains more history, which the steady_records metric makes
// visible rather than hiding.
//
// MaxWait for the uncontended family just covers ~3 gate round trips (2x
// SubmitWindow with a 30s floor). For the contended family that is not
// enough: queue depth dominates. Model (bench-auditor, empirically confirmed
// by BenchmarkSharedContended/W=256/H=0: predicted mean cycle W^2*tc/8 =
// 7.9s vs measured 5.96s): the gate stays saturated because W queued workers
// offer an Observe every PollInterval, so Submit/Release only receive about
// 1/(k+2) of the service share (k polls per cycle). A waiter at depth d is
// granted after ~d/capacity slot turnovers, each costing a full gate round
// trip W*tc, giving a worst-case wait of about W^2*(k+2)*tc/4. MaxWait
// therefore takes max(30s, 2x SubmitWindow, 1.25 * W^2 * tc(records+3W));
// the 1.25 factor sits mid-range of the 1.0-1.5 constant band and keeps a
// ~5x margin over the plain W^2*tc/4 worst case (W^2*tc/8 mean). At the
// matrix extremes this stays under 50 minutes, below both the protocol's 1h
// cap and sharedBenchMaxWaitSkipThreshold, so no current cell skips.
//
// OperationTimeout bounds one driver call, so it covers one gate backlog
// plus slack, capped at the one-minute protocol limit (W=256 at the largest
// clamped H derives ~40s, still inside the cap).
func sharedBenchWindows(workers, records int, contended bool) (submitWindow, maxWait, operationTimeout time.Duration) {
	gate := time.Duration(workers) * sharedBenchTransitionEstimate(records)
	submitWindow = benchCeilMillis(max(sharedBenchSubmitWindowFloor, min(4*gate, sharedBenchSubmitWindowCeil)))
	maxWait = max(30*time.Second, 2*submitWindow)
	if contended {
		// 1.25 * W^2 * tc == 5 * W * gate / 4, in exact rational arithmetic.
		maxWait = max(maxWait, benchCeilMillis(5*gate*time.Duration(workers)/4))
	}
	operationTimeout = min(time.Minute, submitWindow+5*time.Second)
	return submitWindow, maxWait, operationTimeout
}

// benchSeedSnapshot builds a valid committed snapshot carrying H terminal seed
// records in O(H), mirroring scaleSnapshot/scaleBytes: per-record Config is
// zeroed before marshal (Transition restores it from the snapshot Config) and
// one probe Transition proves the fixture restorable. A nil seed starts the
// resource empty.
func benchSeedSnapshot(tb testing.TB, resource string, cfg ResourceConfig, terminals int) []byte {
	tb.Helper()
	if terminals < 0 || terminals >= cfg.MaxRecords {
		tb.Fatal("invalid seed terminal count")
	}
	if terminals == 0 {
		return nil
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	s := durableSnapshot{Version: 1, Resource: resource, Config: cfg,
		Records: make(map[string]*durableRecord, terminals), Queues: make(map[string][]string),
		Rotations: [][]string{{}}, Revision: 1, Time: now}
	var probe Request
	until := now.Add(time.Hour)
	for i := 0; i < terminals; i++ {
		r := Request{Resource: resource, ID: fmt.Sprintf("%d-%032x", until.UnixMilli(), i+1),
			Owner: fmt.Sprintf("%032x", i+1), SubmitUntil: until, WaitUntil: until,
			Weight: 1, Config: cfg}
		s.Records[r.ID] = &durableRecord{Request: r, Status: Cancelled, Finished: now}
		if i == 0 {
			probe = r
		}
	}
	for _, rec := range s.Records {
		rec.Request.Config = ResourceConfig{}
	}
	data, err := json.Marshal(s)
	if err != nil {
		tb.Fatal(err)
	}
	if next, _, err := transitionSnapshot(data, now, Command{Kind: Observe, Request: probe}); err != nil || next == nil {
		tb.Fatalf("invalid seed fixture: %v", err)
	}
	return data
}

// sharedBenchParams fixes one matrix point. workers is W (and the tenant
// count), cycles is the global cycle budget (b.N in benchmarks), capacity and
// maxQueued shape admission, terminals is the H seed target.
type sharedBenchParams struct {
	resource  string
	workers   int
	cycles    int64
	capacity  int
	maxQueued int
	terminals int
}

// sharedBenchEnv owns all mutable state of one run. The b.Run body (and thus
// this env) is fully rebuilt for every b.N re-run: driver, seed, client and
// latency buffers never carry state across rounds.
type sharedBenchEnv struct {
	driver    *benchDriver
	client    *Shared
	params    sharedBenchParams
	seedCount int
	// tb receives ErrCorruptState diagnostics from worker goroutines; only
	// its concurrency-safe Log methods are used outside the bench goroutine.
	tb       testing.TB
	ctx      context.Context
	stop     context.CancelFunc
	cursor   atomic.Int64
	abort    atomic.Bool
	failures chan error
}

// newSharedBenchEnv builds the fixture, driver and client. The single cfg
// value feeds both SharedOptions.Config and the seed snapshot; Transition
// DeepEqual-checks the two, so sharing one value guarantees no
// ErrConfigConflict (requirement: fixture Config must match exactly).
func newSharedBenchEnv(tb testing.TB, p sharedBenchParams) *sharedBenchEnv {
	tb.Helper()
	if p.workers < 1 || p.cycles < 1 || p.capacity < 1 || p.capacity > 10000 || p.maxQueued < 0 || p.maxQueued > 10000 {
		tb.Fatal("invalid shared bench parameters")
	}
	cfg := ResourceConfig{
		Admission: Config{Capacity: p.capacity, MaxQueued: p.maxQueued,
			MaxQueuedPerTenant: min(2, p.maxQueued)},
		LeaseTTL:   sharedBenchLeaseTTL,
		Retention:  sharedBenchRetention,
		MaxRecords: sharedBenchMaxRecords,
	}
	terminals := p.terminals
	// Clamp only bites at H=10000 (every other point fits seeds plus the
	// worst accumulation with room to spare): MaxRecords caps seeds plus live
	// traffic, and a full seed table would fail every Submit with
	// ErrHistoryFull. The clamp keeps the deployment-limit dimension runnable;
	// seed_records reports the actual seed count.
	headroom := sharedBenchHeadroomBase + sharedBenchHeadroomPerWorker*p.workers
	if limit := cfg.MaxRecords - headroom; terminals > limit {
		tb.Logf("H=%d clamped to %d seed records: MaxRecords=%d reserves headroom %d for in-flight and unswept terminals",
			terminals, limit, cfg.MaxRecords, headroom)
		terminals = limit
	}
	// Window inputs count seeds plus in-flight requests and the steady-state
	// terminal accumulation (~2W) so the estimate reflects stressed history.
	// The contended flag (maxQueued > 0) switches MaxWait to the queue-depth
	// model; cells whose derived wait cannot fit under the protocol cap are
	// skipped loudly rather than measured invalid.
	submitWindow, maxWait, operationTimeout := sharedBenchWindows(p.workers, terminals+3*p.workers, p.maxQueued > 0)
	if maxWait > sharedBenchMaxWaitSkipThreshold {
		tb.Skipf("W=%d/H=%d: derived MaxWait %s exceeds %s; steady-state drain is unmeasurable within protocol limits",
			p.workers, p.terminals, maxWait, sharedBenchMaxWaitSkipThreshold)
	}
	cfg.MaxWait = maxWait
	cfg.SubmitWindow = submitWindow
	driver := newBenchDriver(benchSeedSnapshot(tb, p.resource, cfg, terminals))
	client, err := NewShared(driver, SharedOptions{Resource: p.resource, Config: cfg,
		OperationTimeout: operationTimeout, PollInterval: sharedBenchPollInterval})
	if err != nil {
		tb.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	return &sharedBenchEnv{driver: driver, client: client, params: p, seedCount: terminals,
		tb: tb, ctx: ctx, stop: stop, failures: make(chan error, p.workers)}
}

// fail records a worker error and aborts sibling workers. It never calls
// Fatal from a worker goroutine: errors travel over a channel and become
// Fatal only in the bench goroutine after the join. When the error chain
// contains ErrCorruptState, dumpCorruptState first logs the committed
// snapshot's clock evidence, so any future sporadic occurrence is
// immediately attributable to (or cleared of) the backward-clock mechanism.
func (e *sharedBenchEnv) fail(slot int, operation string, err error) {
	e.abort.Store(true)
	e.stop()
	if errors.Is(err, ErrCorruptState) {
		e.dumpCorruptState(slot, operation)
	}
	select {
	case e.failures <- fmt.Errorf("worker %d: %s: %w", slot, operation, err):
	default:
	}
}

// dumpCorruptState logs the committed snapshot's revision, record count,
// backend timestamp and its signed distance to the local clock. A negative
// delta (clock behind the last committed time) confirms the fail-closed
// backward-clock mechanism; a nonnegative delta points at real corruption
// and deserves a fresh investigation. The snapshot is read under the
// driver's gate (with a bounded wait) because sibling workers may still be
// mid-transition; tb.Logf is safe from this goroutine.
func (e *sharedBenchEnv) dumpCorruptState(slot int, operation string) {
	select {
	case e.driver.gate <- struct{}{}:
		defer func() { <-e.driver.gate }()
	case <-time.After(time.Second):
		e.tb.Logf("worker %d: %s: corrupt-state dump skipped: driver gate still busy", slot, operation)
		return
	}
	snapshot := e.driver.snapshot
	if len(snapshot) == 0 {
		e.tb.Logf("worker %d: %s: corrupt-state dump: snapshot empty", slot, operation)
		return
	}
	var s durableSnapshot
	if err := json.Unmarshal(snapshot, &s); err != nil {
		e.tb.Logf("worker %d: %s: corrupt-state dump: snapshot unreadable: %v", slot, operation, err)
		return
	}
	now := time.Now()
	e.tb.Logf("worker %d: %s: corrupt-state dump: revision=%d records=%d snapshot_time=%s now=%s delta=%s (negative delta = backward wall clock, see TestSharedBenchBackwardClockFailsClosed)",
		slot, operation, s.Revision, len(s.Records), s.Time.UTC().Format(time.RFC3339Nano),
		now.UTC().Format(time.RFC3339Nano), now.Sub(s.Time))
}

// sharedBenchStats aggregates one completed run; latencies is sorted.
type sharedBenchStats struct {
	completed     int64
	latencies     []time.Duration
	steadyRecords int
	seedRecords   int
}

// run executes the cycle budget with W long-lived workers. Cycles are claimed
// from one atomic counter, so the global count is exact and idle workers exit
// immediately. Worker startup (spawn until parked) and the join stay outside
// the timed region via ResetTimer/StopTimer; the hot path never stops the
// timer per cycle. The claim number doubles as the index into one sample
// array allocated before the timer starts: every completed cycle writes its
// own slot (exactly one writer per slot, no lock, no allocation), so the
// timed region carries no harness bookkeeping and no merge pass is needed:
// a worker only skips its slot on a failure, and any failure is Fatal before
// stats are read. Sorting happens after the join.
func (e *sharedBenchEnv) run(tb testing.TB) sharedBenchStats {
	samples := make([]time.Duration, e.params.cycles)
	var ready, finished sync.WaitGroup
	start := make(chan struct{})
	for slot := range e.params.workers {
		ready.Add(1)
		finished.Add(1)
		go func(slot int) {
			defer finished.Done()
			tenant := fmt.Sprintf("bench-worker-%04d", slot)
			ready.Done()
			<-start
			for !e.abort.Load() {
				seq := e.cursor.Add(1)
				if seq > e.params.cycles {
					return
				}
				cycleStart := time.Now()
				lease, err := e.client.AcquireFor(e.ctx, tenant)
				if err != nil {
					e.fail(slot, "acquire", err)
					return
				}
				// Acquire latency only: sample before the release round trip.
				waited := time.Since(cycleStart)
				if err = lease.Release(context.Background()); err != nil {
					e.fail(slot, "release", err)
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
	e.stop()
	close(e.failures)
	for err := range e.failures {
		tb.Fatal(err)
	}
	stats := sharedBenchStats{completed: int64(len(samples)), latencies: samples, seedRecords: e.seedCount}
	slices.Sort(stats.latencies)
	var snapshot durableSnapshot
	if len(e.driver.snapshot) > 0 {
		if err := json.Unmarshal(e.driver.snapshot, &snapshot); err != nil {
			tb.Fatal(err)
		}
		stats.steadyRecords = len(snapshot.Records)
	}
	return stats
}

// benchPercentile uses the nearest-rank method on sorted samples: rank is
// ceil(q*N), clamped to [1,N], and the result is sorted[rank-1].
func benchPercentile(sorted []time.Duration, q float64) time.Duration {
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

// reportSharedBenchMetrics publishes the run's custom metrics. With no seed
// holders, effective_capacity always equals the configured Capacity.
func reportSharedBenchMetrics(b *testing.B, p sharedBenchParams, stats sharedBenchStats) {
	if seconds := b.Elapsed().Seconds(); seconds > 0 {
		b.ReportMetric(float64(stats.completed)/seconds, "cycles_per_s")
	}
	for _, q := range []struct {
		fraction float64
		key      string
	}{{0.50, "latency_p50_ns"}, {0.95, "latency_p95_ns"}, {0.99, "latency_p99_ns"}, {0.999, "latency_p999_ns"}} {
		b.ReportMetric(float64(benchPercentile(stats.latencies, q.fraction)), q.key)
	}
	if n := len(stats.latencies); n > 0 {
		b.ReportMetric(float64(stats.latencies[n-1]), "latency_max_ns")
	}
	b.ReportMetric(float64(stats.steadyRecords), "steady_records")
	b.ReportMetric(float64(stats.seedRecords), "seed_records")
	b.ReportMetric(float64(p.capacity), "effective_capacity")
}

// benchmarkSharedMatrix runs the W x H matrix for one family. The default
// matrix stays small; AIRLOCK_BENCH_LARGE=1 appends H=10000 (deployment
// limit, seed-clamped as documented above). Uncontended sets Capacity=W: a
// worker submits only while holding nothing, so peers hold at most W-1 tokens
// and every Submit is granted without queuing; MaxQueued=0 then makes any
// accidental queuing fail loudly as ErrQueueFull instead of silently
// distorting the "Submit is Granted" contract. Contended pins Capacity=8 and
// queues the rest.
func benchmarkSharedMatrix(b *testing.B, contended bool) {
	workerCounts := []int{1, 4, 16, 64, 256}
	historyCounts := []int{0, 128, 1000, 5000}
	if os.Getenv("AIRLOCK_BENCH_LARGE") == "1" {
		historyCounts = append(historyCounts, 10000)
	}
	for _, workers := range workerCounts {
		for _, history := range historyCounts {
			// Sub-benchmark names are slash-separated (W=16/H=5000). Filter
			// with that exact form and an anchor ('W=16/H=5000$'): a hyphen
			// form ('W=16-H=5000') matches nothing and silently runs an
			// empty parent, and Go bench regexes are unanchored, so an
			// alternation like 'W=(1|4)/' also prefix-matches W=16/W=4x.
			b.Run(fmt.Sprintf("W=%d/H=%d", workers, history), func(b *testing.B) {
				p := sharedBenchParams{resource: "shared-bench", workers: workers,
					cycles: int64(b.N), capacity: workers, maxQueued: 0, terminals: history}
				if contended {
					p.resource = "shared-bench-contended"
					p.capacity = sharedBenchContendedCapacity
					p.maxQueued = workers + 16
				}
				env := newSharedBenchEnv(b, p)
				b.ReportAllocs()
				stats := env.run(b)
				reportSharedBenchMetrics(b, p, stats)
			})
		}
	}
}

// BenchmarkSharedUncontended measures Submit-is-Granted throughput and Acquire
// latency with capacity for every worker (no queuing, no polling).
func BenchmarkSharedUncontended(b *testing.B) { benchmarkSharedMatrix(b, false) }

// BenchmarkSharedContended measures queued acquisition under a fixed capacity
// of 8: workers queue, poll every PollInterval and drain through dispatch.
func BenchmarkSharedContended(b *testing.B) { benchmarkSharedMatrix(b, true) }

// TestSharedBenchmarkSmoke exercises the harness with a few cycles for the
// race-detector gate. Every cycle must succeed; steady_records must retain
// the seeds and stay within seeds+cycles, because each cycle creates at most
// one record and terminals younger than SubmitWindow are legitimately still
// present at inspection time.
func TestSharedBenchmarkSmoke(t *testing.T) {
	for _, p := range []sharedBenchParams{
		{resource: "smoke-uncontended", workers: 4, cycles: 64, capacity: 4, maxQueued: 0, terminals: 8},
		{resource: "smoke-contended", workers: 4, cycles: 64, capacity: 2, maxQueued: 20, terminals: 8},
	} {
		stats := newSharedBenchEnv(t, p).run(t)
		if stats.completed != p.cycles || len(stats.latencies) != int(p.cycles) {
			t.Fatalf("%s: completed %d of %d cycles (latency samples %d)",
				p.resource, stats.completed, p.cycles, len(stats.latencies))
		}
		if stats.steadyRecords < stats.seedRecords || stats.steadyRecords > stats.seedRecords+int(p.cycles) {
			t.Fatalf("%s: steady records %d outside [%d, %d]",
				p.resource, stats.steadyRecords, stats.seedRecords, stats.seedRecords+int(p.cycles))
		}
	}
}

// TestSharedBenchWindows pins the window derivation (the audit-B1 fix): every
// output is a whole millisecond (protocol requirement), SubmitWindow stays
// within its documented floor/ceiling, OperationTimeout respects the one-minute
// cap, MaxWait equals exactly the documented formula (the uncontended floors
// max(30s, 2x SubmitWindow), plus the contended queue-depth term
// 1.25*W^2*transition(records)), and the worst current matrix point stays
// below the skip threshold while a deeper hypothetical point crosses it, so
// uncoverable cells skip loudly instead of running invalid.
func TestSharedBenchWindows(t *testing.T) {
	for _, p := range []struct {
		workers, records int
		contended        bool
	}{
		{workers: 1, records: 0, contended: false},
		{workers: 4, records: 128, contended: false},
		{workers: 16, records: 1000, contended: true},
		{workers: 64, records: 5000, contended: true},
	} {
		submitWindow, maxWait, operationTimeout := sharedBenchWindows(p.workers, p.records, p.contended)
		if submitWindow%time.Millisecond != 0 || maxWait%time.Millisecond != 0 || operationTimeout%time.Millisecond != 0 {
			t.Fatalf("W=%d/R=%d: sub-millisecond window %s/%s/%s",
				p.workers, p.records, submitWindow, maxWait, operationTimeout)
		}
		if submitWindow < sharedBenchSubmitWindowFloor || submitWindow > sharedBenchSubmitWindowCeil {
			t.Fatalf("W=%d/R=%d: SubmitWindow %s outside [%s, %s]",
				p.workers, p.records, submitWindow, sharedBenchSubmitWindowFloor, sharedBenchSubmitWindowCeil)
		}
		if operationTimeout > time.Minute {
			t.Fatalf("W=%d/R=%d: OperationTimeout %s exceeds one minute", p.workers, p.records, operationTimeout)
		}
		want := max(30*time.Second, 2*submitWindow)
		if p.contended {
			gate := time.Duration(p.workers) * sharedBenchTransitionEstimate(p.records)
			want = max(want, benchCeilMillis(5*gate*time.Duration(p.workers)/4))
		}
		if maxWait != want {
			t.Fatalf("W=%d/R=%d contended=%t: MaxWait %s, want %s", p.workers, p.records, p.contended, maxWait, want)
		}
	}
	// Worst current matrix point: W=256 with H clamped to the MaxRecords
	// headroom (10000 - 1024 - 6*256 = 7440 seeds) plus 3W live records
	// derives ~47 minutes and must stay under the skip threshold.
	clamped := sharedBenchMaxRecords - (sharedBenchHeadroomBase + sharedBenchHeadroomPerWorker*256)
	if _, maxWait, _ := sharedBenchWindows(256, clamped+3*256, true); maxWait > sharedBenchMaxWaitSkipThreshold {
		t.Fatalf("worst matrix point derives MaxWait %s, exceeds skip threshold %s",
			maxWait, sharedBenchMaxWaitSkipThreshold)
	}
	// A deeper hypothetical point (W=512 at 9000 records, ~2.9h) must cross
	// the threshold so such a cell would skip loudly.
	if _, maxWait, _ := sharedBenchWindows(512, 9000, true); maxWait <= sharedBenchMaxWaitSkipThreshold {
		t.Fatalf("hypothetical deep point derives MaxWait %s, stays under skip threshold %s",
			maxWait, sharedBenchMaxWaitSkipThreshold)
	}
}

// TestBenchPercentile pins the nearest-rank percentile used by the latency
// metrics: rank ceil(q*N) clamped to [1,N] on the ascending samples.
func TestBenchPercentile(t *testing.T) {
	sorted := []time.Duration{1, 2, 3, 4, 8}
	for _, p := range []struct {
		q    float64
		want time.Duration
	}{
		{0.50, 3},  // ceil(2.5) = rank 3
		{0.80, 4},  // ceil(4.0) = rank 4
		{0.95, 8},  // ceil(4.75) = rank 5
		{0.999, 8}, // clamped to N
		{0.0, 1},   // rank clamped up to 1
		{1.0, 8},
	} {
		if got := benchPercentile(sorted, p.q); got != p.want {
			t.Fatalf("benchPercentile(q=%g) = %d, want %d", p.q, got, p.want)
		}
	}
	if got := benchPercentile(nil, 0.99); got != 0 {
		t.Fatalf("benchPercentile(empty) = %d, want 0", got)
	}
}

// benchBackwardClock serves the real wall time, then steps its baseline
// backward by step once more than jumpAfter calls have been served. It makes
// the protocol's fail-closed backward-clock check deterministic.
type benchBackwardClock struct {
	mu        sync.Mutex
	calls     int
	jumpAfter int
	step      time.Duration
}

// now returns the (possibly stepped-back) current backend time.
func (c *benchBackwardClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls > c.jumpAfter {
		return time.Now().Add(-c.step)
	}
	return time.Now()
}

// TestSharedBenchBackwardClockFailsClosed proves the audit-B2 mechanism: the
// sporadic ErrCorruptState seen on darwin is the driver contract's fail-closed
// backward-clock check, not state corruption. Transition truncates each
// operation's backend time to whole milliseconds and compares it against the
// snapshot's last committed time (shared_state.go: now.Before(s.Time)), so a
// clock that steps backward by at least 1ms necessarily crosses a millisecond
// boundary below the committed time and fails closed; a sub-millisecond
// backward step can be absorbed by truncation (both times flooring into the
// same millisecond), which is why the production sightings are sporadic
// rather than constant. The stepped clock fails the release of the second
// cycle here (the first transition after the jump), and the committed
// snapshot survives because Transition returns no new state on this error.
func TestSharedBenchBackwardClockFailsClosed(t *testing.T) {
	d := newBenchDriver(nil)
	d.nowFunc = (&benchBackwardClock{jumpAfter: 3, step: 2 * time.Millisecond}).now
	cfg := ResourceConfig{
		Admission:    Config{Capacity: 1},
		LeaseTTL:     30 * time.Second,
		MaxWait:      time.Second,
		SubmitWindow: 10 * time.Millisecond,
		Retention:    time.Millisecond,
		MaxRecords:   100,
	}
	client, err := NewShared(d, SharedOptions{Resource: "backward-clock", Config: cfg,
		OperationTimeout: 5 * time.Second, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		lease, cycleErr := client.AcquireFor(context.Background(), "clock")
		if cycleErr == nil {
			cycleErr = lease.Release(context.Background())
		}
		if cycleErr != nil {
			if !errors.Is(cycleErr, ErrCorruptState) {
				t.Fatalf("cycle %d: want ErrCorruptState, got %v", i, cycleErr)
			}
			var snapshot durableSnapshot
			if err := json.Unmarshal(d.snapshot, &snapshot); err != nil || snapshot.Revision == 0 {
				t.Fatalf("cycle %d: committed snapshot lost: %v", i, err)
			}
			return
		}
	}
	t.Fatal("backward clock never produced ErrCorruptState")
}

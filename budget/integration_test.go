package budget

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

// poolHarness is an in-process airlock.Driver backing the shared budget pool.
// It mirrors the clientStateDriver pattern from the root package tests but
// relies only on exported airlock APIs: a single mutex models the backend's
// per-resource atomic boundary around one authoritative *airlock.SharedState,
// and every method is a thin Command wrapper over airlock.Transition. It also
// implements airlock.ResourceManager so Shared clients (and the tests) can
// Resize and Inspect the pool. It is safe for concurrent use.
type poolHarness struct {
	mu    sync.Mutex
	state *airlock.SharedState
}

// execute runs one transition against the harness state under the lock,
// persisting every nonnil next state even alongside business errors.
func (h *poolHarness) execute(ctx context.Context, cmd airlock.Command) (airlock.Result, error) {
	if err := ctx.Err(); err != nil {
		return airlock.Result{}, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	next, result, _, err := airlock.Transition(h.state, time.Now().UTC(), cmd)
	if next != nil {
		h.state = next
	}

	return result, err
}

// Submit admits or queues the whole request atomically.
func (h *poolHarness) Submit(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return h.execute(ctx, airlock.Command{Kind: airlock.Submit, Request: request})
}

// Observe reads outcomes and advances due maintenance.
func (h *poolHarness) Observe(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return h.execute(ctx, airlock.Command{Kind: airlock.Observe, Request: request})
}

// Cancel terminates queued or granted requests.
func (h *poolHarness) Cancel(ctx context.Context, request airlock.Request) (airlock.Result, error) {
	return h.execute(ctx, airlock.Command{Kind: airlock.Cancel, Request: request})
}

// Renew extends a live matching lease.
func (h *poolHarness) Renew(ctx context.Context, lease airlock.LeaseRef) (airlock.Result, error) {
	return h.execute(ctx, airlock.Command{Kind: airlock.Renew, Request: lease.Request, LeaseID: lease.ID})
}

// Release ends a matching lease exactly once.
func (h *poolHarness) Release(ctx context.Context, lease airlock.LeaseRef) (airlock.Result, error) {
	return h.execute(ctx, airlock.Command{Kind: airlock.Release, Request: lease.Request, LeaseID: lease.ID})
}

// Resize adjusts pool capacity under an expected capacity version.
func (h *poolHarness) Resize(ctx context.Context, request airlock.ResizeRequest) (airlock.ResourceStatus, error) {
	result, err := h.execute(ctx, airlock.Command{Kind: airlock.Resize, Resize: request})
	if err != nil {
		return airlock.ResourceStatus{}, err
	}

	return result.ResourceStatus, nil
}

// InspectResource reads the committed snapshot without maintenance.
func (h *poolHarness) InspectResource(ctx context.Context, name string) (airlock.ResourceStatus, error) {
	if err := ctx.Err(); err != nil {
		return airlock.ResourceStatus{}, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.state == nil {
		return airlock.ResourceStatus{}, airlock.ErrResourceNotFound
	}

	observation, err := h.state.Observation()
	if err != nil {
		return airlock.ResourceStatus{}, err
	}

	return observation.Inspect(time.Now().UTC(), name)
}

// capacityVersion returns the committed CapacityVersion, or zero before the
// resource exists.
func (h *poolHarness) capacityVersion() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.state == nil {
		return 0
	}

	return h.state.CapacityVersion
}

// Interface guards: the harness is a full Driver plus ResourceManager.
var (
	_ airlock.Driver          = (*poolHarness)(nil)
	_ airlock.ResourceManager = (*poolHarness)(nil)
)

// Integration timing knobs: short but real deadlines so the suite survives
// -race -count=10 -cpu=1,2,4 without precise timing assumptions.
const (
	testPoolCapacity = 4
	testLeaseTTL     = 600 * time.Millisecond
	testInterval     = 50 * time.Millisecond
	testResource     = "budget-acceptance-pool"
	testAcquireWait  = 400 * time.Millisecond
)

// integrationOptions returns the stage-1 acceptance configuration: a tiny
// pool, short lease TTL, fast control loop, and {2,1} denominations.
func integrationOptions(driver airlock.Driver, gateway string) Options {
	return Options{
		Driver:    driver,
		Resource:  testResource,
		GatewayID: gateway,
		Policy: airlock.ResourceConfig{
			Admission: airlock.Config{
				Capacity:           testPoolCapacity,
				MaxQueued:          64,
				MaxQueuedPerTenant: 64,
			},
			LeaseTTL: testLeaseTTL,
		},
		Interval:         testInterval,
		Denominations:    []int{2, 1},
		MaxChunks:        8,
		MinHeld:          0,
		MaxHeld:          testPoolCapacity,
		Headroom:         1.3,
		LowUtilization:   0.5,
		LowPeriods:       2,
		LocalQueue:       8,
		OperationTimeout: 500 * time.Millisecond,
	}
}

// newIntegrationManager builds a Manager over the pool or fails the test.
func newIntegrationManager(t *testing.T, driver airlock.Driver, gateway string) *Manager {
	t.Helper()
	m, err := New(integrationOptions(driver, gateway))
	if err != nil {
		t.Fatalf("gateway %s: New: %v", gateway, err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testAcquireWait)
		defer cancel()

		_ = m.Close(ctx)
	})

	return m
}

// eventually polls check every interval until it holds or the deadline passes.
func eventually(t *testing.T, deadline time.Duration, interval time.Duration, description string, check func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		if check() {
			return
		}

		if !time.Now().Before(end) {
			t.Fatalf("condition not met within %s: %s", deadline, description)
		}

		time.Sleep(interval)
	}
}

// startWorkers launches n worker goroutines on m, each looping Acquire(1) with
// short random holds until stop closes. The returned function waits for them.
func startWorkers(m *Manager, gateway string, n int, stop <-chan struct{}, accounting ...func(int)) func() {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(seed int64, worker int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(seed))
			tenant := gateway + "-worker"
			for {
				select {
				case <-stop:
					return
				default:
				}

				ctx, cancel := context.WithTimeout(context.Background(), testAcquireWait)
				permit, err := m.Acquire(ctx, tenant, 1)
				cancel()
				if err != nil {
					continue
				}

				if len(accounting) > 0 {
					accounting[0](1)
				}

				time.Sleep(time.Duration(1+rng.Intn(8)) * time.Millisecond)
				if len(accounting) > 0 {
					accounting[0](-1)
				}

				permit.Release()
			}
		}(time.Now().UnixNano()+int64(i), i)
	}

	return wg.Wait
}

// TestIntegrationSafetyAcrossGateways counts delivered simulated work with one
// atomic counter across gateways. Deterministic operation-barrier regressions
// separately exercise return/close boundaries; this load test is not a proof of
// every interleaving or of safety after TTL reallocation.
func TestIntegrationSafetyAcrossGateways(t *testing.T) {
	pool := &poolHarness{}
	a := newIntegrationManager(t, pool, "gw-a")
	b := newIntegrationManager(t, pool, "gw-b")
	var active, peak atomic.Int64
	var exceeded atomic.Bool
	account := func(delta int) {
		n := active.Add(int64(delta))
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}

		if n > testPoolCapacity {
			exceeded.Store(true)
		}
	}

	stop := make(chan struct{})
	waitA := startWorkers(a, "a", 4, stop, account)
	waitB := startWorkers(b, "b", 4, stop, account)
	time.Sleep(2 * time.Second)
	close(stop)
	waitA()
	waitB()
	if exceeded.Load() {
		t.Fatalf("delivered work exceeded capacity: peak=%d", peak.Load())
	}

	if active.Load() != 0 || peak.Load() == 0 {
		t.Fatalf("active=%d peak=%d", active.Load(), peak.Load())
	}
}

// TestIntegrationWorkConserving checks that a fully loaded gateway grows its
// chunk holding while the pool is otherwise idle: eventually A holds at least
// two tokens, proving spare pool budget migrates to demand.
func TestIntegrationWorkConserving(t *testing.T) {
	pool := &poolHarness{}
	a := newIntegrationManager(t, pool, "gw-a")
	_ = newIntegrationManager(t, pool, "gw-b") // idle gateway shares the pool

	stop := make(chan struct{})
	wait := startWorkers(a, "a", 4, stop)
	defer func() {
		close(stop)
		wait()
	}()

	eventually(t, 3*time.Second, 10*time.Millisecond, "loaded gateway should hold at least 2 tokens", func() bool {
		return a.Held() >= 2
	})
}

// TestIntegrationIdleReturn checks that a gateway which obtained chunks and
// then went idle returns its whole holding to the pool within a generous
// window around LowPeriods x Interval.
func TestIntegrationIdleReturn(t *testing.T) {
	pool := &poolHarness{}
	b := newIntegrationManager(t, pool, "gw-b")

	stop := make(chan struct{})
	wait := startWorkers(b, "b", 2, stop)

	eventually(t, 3*time.Second, 10*time.Millisecond, "gateway should obtain at least one chunk under load", func() bool {
		return b.Held() >= 1
	})

	close(stop)
	wait()

	eventually(t, 4*time.Second, 10*time.Millisecond, "idle gateway should return all chunks to the pool", func() bool {
		return b.Held() == 0
	})
}

// TestIntegrationResizeZeroBlocksNewChunks checks Resize(capacity=0): after
// the pool is banned, a fresh gateway with live demand never obtains a new
// chunk and its local admission stops (TryAcquire fails), while permits
// granted before the ban still Release cleanly.
func TestIntegrationResizeZeroBlocksNewChunks(t *testing.T) {
	pool := &poolHarness{}
	a := newIntegrationManager(t, pool, "gw-a")

	stop := make(chan struct{})
	wait := startWorkers(a, "a", 2, stop)

	eventually(t, 3*time.Second, 10*time.Millisecond, "gateway should hold chunks before the ban", func() bool {
		return a.Held() >= 2
	})

	// Keep one permit outstanding across the ban.
	ctx, cancel := context.WithTimeout(context.Background(), testAcquireWait)
	permit, err := a.Acquire(ctx, "drain-tenant", 1)
	cancel()
	if err != nil {
		close(stop)
		wait()
		t.Fatalf("pre-ban Acquire failed: %v", err)
	}

	// Ban the pool via the harness ResourceManager with the current version.
	resizeCtx, resizeCancel := context.WithTimeout(context.Background(), testAcquireWait)
	status, err := pool.Resize(resizeCtx, airlock.ResizeRequest{
		Resource:        testResource,
		Capacity:        0,
		ExpectedVersion: pool.capacityVersion(),
	})
	resizeCancel()
	if err != nil {
		t.Fatalf("Resize(0): %v", err)
	}

	if status.Capacity != 0 {
		t.Fatalf("Resize(0) reported capacity %d", status.Capacity)
	}

	// Fresh gateway with live demand must never obtain a chunk while banned.
	c := newIntegrationManager(t, pool, "gw-c")
	demandStop := make(chan struct{})
	demandWait := startWorkers(c, "c", 2, demandStop)

	// The banned gateway must stay at zero held weight despite live demand
	// for a window spanning many control-loop intervals.
	bannedSince := time.Now()
	for time.Since(bannedSince) < 10*testInterval {
		if held := c.Held(); held > 0 {
			close(demandStop)
			demandWait()
			t.Fatalf("banned gateway acquired chunks: Held=%d", held)
		}

		time.Sleep(10 * time.Millisecond)
	}

	// Local admission on the banned gateway stops: TryAcquire must fail once
	// the control loop has resized the local Airlock to zero.
	eventually(t, 3*time.Second, 10*time.Millisecond, "banned gateway local admission should stop", func() bool {
		_, err := c.TryAcquire("drain-tenant", 1)

		return err != nil
	})

	// Stop demand and confirm the pre-ban permit releases cleanly.
	close(demandStop)
	demandWait()

	permit.Release()

	ctx2, cancel2 := context.WithTimeout(context.Background(), testAcquireWait)
	defer cancel2()

	if err := a.Close(ctx2); err != nil {
		t.Fatalf("Close after drain: %v", err)
	}

	close(stop)
	wait()
}

// TestIntegrationManagerCloseIdempotent guards lifecycle basics: Close is
// idempotent and read-only calls stay safe afterwards.
func TestIntegrationManagerCloseIdempotent(t *testing.T) {
	pool := &poolHarness{}
	m := newIntegrationManager(t, pool, "gw-x")

	ctx, cancel := context.WithTimeout(context.Background(), testAcquireWait)
	defer cancel()

	if err := m.Close(ctx); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	if err := m.Close(ctx); err != nil {
		t.Fatalf("second Close should be idempotent, got %v", err)
	}

	if p, err := m.TryAcquire("post-close", 1); err == nil {
		p.Release()
		t.Fatal("post-close admission succeeded")
	}

	if m.Stats().Capacity < 0 {
		t.Fatalf("negative capacity after close: %+v", m.Stats())
	}
}

package budget

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

// fakeLease is a deterministic chunkLease with injectable failures.
type fakeLease struct {
	weight     int
	src        *fakeSource
	mu         sync.Mutex
	renewErr   error
	releaseErr error
	renewed    int
	released   int
	invalid    bool
}

func (l *fakeLease) Weight() int { return l.weight }
func (l *fakeLease) Valid() bool { l.mu.Lock(); defer l.mu.Unlock(); return !l.invalid }

func (l *fakeLease) Renew(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.renewed++
	if l.renewErr != nil {
		l.invalid = true
	}
	return l.renewErr
}

func (l *fakeLease) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released++
	if l.released == 1 && l.src != nil {
		l.src.returned(l.weight)
	}
	return l.releaseErr
}

// fakeSource is a deterministic chunkSource. blocked makes every Acquire fail
// with airlock.ErrWeightExceedsCapacity; maxWeight (when positive) rejects
// heavier chunks the same way; capacity (when positive) models the pool total:
// an Acquire that would push live held weight above it is rejected the same
// way; exhausted free capacity returns a wait deadline, as real Shared does.
// err fails with a plain backend error.
type fakeSource struct {
	mu        sync.Mutex
	blocked   bool
	maxWeight int
	capacity  int
	live      int
	err       error
	acquired  []int
	leases    []*fakeLease
}

func (f *fakeSource) Acquire(ctx context.Context, tenant string, weight int) (chunkLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blocked || (f.maxWeight > 0 && weight > f.maxWeight) {
		return nil, airlock.ErrWeightExceedsCapacity
	}
	if f.capacity > 0 && f.live+weight > f.capacity {
		return nil, context.DeadlineExceeded
	}
	if f.err != nil {
		return nil, f.err
	}
	lease := &fakeLease{weight: weight, src: f}
	f.live += weight
	f.acquired = append(f.acquired, weight)
	f.leases = append(f.leases, lease)
	return lease, nil
}

// returned accounts a first Release returning its weight to the fake pool.
func (f *fakeSource) returned(weight int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.live -= weight
}

func (f *fakeSource) setBlocked(b bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocked = b
}

func (f *fakeSource) weights() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.acquired...)
}

// baseOpts returns valid options for fake-source tests; MaxHeld is always set
// explicitly so a zero Policy capacity cannot matter.
func baseOpts() Options {
	return Options{
		Resource:       "res",
		GatewayID:      "gw",
		Interval:       time.Hour,
		MinHeld:        0,
		MaxHeld:        64,
		Headroom:       1.3,
		LowUtilization: 0.5,
		LowPeriods:     2,
		LocalQueue:     8,
	}
}

func mustManager(t *testing.T, src chunkSource, opts Options) *Manager {
	t.Helper()
	m, err := newManager(src, opts)
	if err != nil {
		t.Fatalf("newManager: %v", err)
	}
	return m
}

func TestNewManagerDefaults(t *testing.T) {
	// Leave every defaulted field at zero so the fill-in branches run.
	opts := Options{Resource: "res", GatewayID: "gw"}
	opts.Policy.Admission.Capacity = 64
	m := mustManager(t, &fakeSource{}, opts)
	if m.opts.Interval != 200*time.Millisecond {
		t.Errorf("Interval = %v, want 200ms", m.opts.Interval)
	}
	if len(m.opts.Denominations) != 3 || m.opts.Denominations[0] != 16 || m.opts.Denominations[1] != 4 || m.opts.Denominations[2] != 1 {
		t.Errorf("Denominations = %v, want [16 4 1]", m.opts.Denominations)
	}
	if m.opts.MaxChunks != 12 {
		t.Errorf("MaxChunks = %d, want 12", m.opts.MaxChunks)
	}
	if m.opts.MaxHeld != 64 {
		t.Errorf("MaxHeld = %d, want 64", m.opts.MaxHeld)
	}
	if m.opts.Headroom != 1.3 {
		t.Errorf("Headroom = %v, want 1.3", m.opts.Headroom)
	}
	if m.opts.LowUtilization != 0.5 {
		t.Errorf("LowUtilization = %v, want 0.5", m.opts.LowUtilization)
	}
	if m.opts.LowPeriods != 2 {
		t.Errorf("LowPeriods = %d, want 2", m.opts.LowPeriods)
	}
	if m.opts.LocalQueue != 8 {
		t.Errorf("LocalQueue = %d, want 8", m.opts.LocalQueue)
	}
	if m.opts.OperationTimeout != 500*time.Millisecond {
		t.Errorf("OperationTimeout = %v, want 500ms", m.opts.OperationTimeout)
	}
}

func TestNewManagerValidation(t *testing.T) {
	opts := func() Options { return baseOpts() }

	if _, err := newManager(nil, opts()); err == nil {
		t.Error("nil source: want error")
	}
	o := opts()
	o.Resource = ""
	if _, err := newManager(&fakeSource{}, o); err == nil {
		t.Error("empty resource: want error")
	}
	o = opts()
	o.GatewayID = ""
	if _, err := newManager(&fakeSource{}, o); err == nil {
		t.Error("empty gateway: want error")
	}
	o = opts()
	o.Denominations = []int{16, 0, 1}
	if _, err := newManager(&fakeSource{}, o); err == nil {
		t.Error("zero denomination: want error")
	}
	o = opts()
	o.MinHeld = 10
	o.MaxHeld = 5
	if _, err := newManager(&fakeSource{}, o); err == nil {
		t.Error("MinHeld > MaxHeld: want error")
	}
	o = opts()
	o.MaxHeld = 0
	if _, err := newManager(&fakeSource{}, o); err == nil {
		t.Error("unset MaxHeld with zero policy capacity: want error")
	}

	if _, err := New(Options{Resource: "r", GatewayID: "g", Policy: airlock.ResourceConfig{Admission: airlock.Config{Capacity: 4}}}); err == nil {
		t.Error("New without driver: want error")
	}
	// An invalid pool policy surfaces the NewShared validation error.
	badPolicy := Options{
		Driver:    &grantDriver{},
		Resource:  "res",
		GatewayID: "gw",
		Policy: airlock.ResourceConfig{
			Admission:    airlock.Config{Capacity: 8},
			LeaseTTL:     5 * time.Second,
			MaxWait:      -time.Second,
			SubmitWindow: time.Second,
			Retention:    time.Minute,
			MaxRecords:   100,
		},
	}
	if _, err := New(badPolicy); err == nil {
		t.Error("New with invalid policy: want error")
	}
	// New fills the operation timeout default before constructing the source.
	m, err := New(Options{
		Driver:    &grantDriver{},
		Resource:  "res",
		GatewayID: "gw",
		Policy: airlock.ResourceConfig{
			Admission:    airlock.Config{Capacity: 8, MaxQueued: 8},
			LeaseTTL:     5 * time.Second,
			MaxWait:      50 * time.Millisecond,
			SubmitWindow: time.Second,
			Retention:    time.Minute,
			MaxRecords:   100,
		},
		Interval:      time.Hour,
		Denominations: []int{4},
		MinHeld:       4,
	})
	if err != nil {
		t.Fatalf("New with default OperationTimeout: %v", err)
	}
	if err := m.Close(context.Background()); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestStepDenominationSelection(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{16, 4, 1}
	opts.MinHeld = 20
	m := mustManager(t, src, opts)
	m.step()
	if got := src.weights(); len(got) != 2 || got[0] != 16 || got[1] != 4 {
		t.Fatalf("acquired %v, want [16 4]", got)
	}
	if m.Held() != 20 {
		t.Errorf("Held = %d, want 20", m.Held())
	}
	if m.Stats().Capacity != 20 {
		t.Errorf("local capacity = %d, want 20", m.Stats().Capacity)
	}
}

func TestStepDenominationsSortedDescending(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{1, 16, 4}
	opts.MinHeld = 21
	m := mustManager(t, src, opts)
	m.step()
	if got := src.weights(); len(got) != 3 || got[0] != 16 || got[1] != 4 || got[2] != 1 {
		t.Fatalf("acquired %v, want [16 4 1]", got)
	}
}

func TestStepNoFittingDenominationStops(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{16}
	opts.MinHeld = 10
	m := mustManager(t, src, opts)
	m.step()
	if m.Held() != 0 {
		t.Errorf("Held = %d, want 0 (no denomination fits)", m.Held())
	}
	if len(src.weights()) != 0 {
		t.Errorf("acquired %v, want none", src.weights())
	}
}

func TestStepMaxChunksCap(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{16}
	opts.MinHeld = 100
	opts.MaxHeld = 128
	opts.MaxChunks = 2
	m := mustManager(t, src, opts)
	m.step()
	if got := src.weights(); len(got) != 2 || got[0] != 16 || got[1] != 16 {
		t.Fatalf("acquired %v, want [16 16]", got)
	}
	if m.Held() != 32 {
		t.Errorf("Held = %d, want 32", m.Held())
	}
}

func TestStepTargetClamp(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{1}
	opts.MinHeld = 3
	m := mustManager(t, src, opts)
	m.step()
	if m.Held() != 3 {
		t.Errorf("Held = %d, want MinHeld 3", m.Held())
	}

	// Upper clamp: queue pressure lifts demand past MaxHeld.
	src2 := &fakeSource{}
	opts2 := baseOpts()
	opts2.Denominations = []int{1}
	opts2.MinHeld = 4
	opts2.MaxHeld = 5
	m2 := mustManager(t, src2, opts2)
	m2.step()
	if m2.Held() != 4 {
		t.Fatalf("Held = %d, want MinHeld 4", m2.Held())
	}
	p, err := m2.Acquire(context.Background(), "t", 4)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	got := make(chan *airlock.Permit, 1)
	go func() {
		permit, err := m2.Acquire(context.Background(), "t", 1)
		if err == nil {
			got <- permit
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for m2.Stats().Queued == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if m2.Stats().Queued == 0 {
		t.Fatal("second Acquire never queued")
	}
	// demand = held(4) + queued(1) = 5, headroom 1.3 -> 7, clamped to MaxHeld 5.
	m2.step()
	if m2.Held() != 5 {
		t.Errorf("Held = %d, want clamped MaxHeld 5", m2.Held())
	}
	p.Release()
	select {
	case permit := <-got:
		permit.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("queued Acquire never returned")
	}
}

func TestStepQueuePressureLiftsDemand(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{1}
	opts.Headroom = 1.0
	m := mustManager(t, src, opts)

	// Admission starts closed: every request queues until a step acquires
	// budget. Three waiters lift demand to held(0)+3 = 3 -> target 3.
	got := make(chan *airlock.Permit, 3)
	for i := 0; i < 3; i++ {
		go func() {
			permit, err := m.Acquire(context.Background(), "t", 1)
			if err == nil {
				got <- permit
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for m.Stats().Queued < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if m.Stats().Queued < 3 {
		t.Fatalf("queued = %d, want 3", m.Stats().Queued)
	}

	m.step()
	if m.Held() != 3 {
		t.Errorf("Held = %d, want 3 lifted by queue pressure", m.Held())
	}
	for i := 0; i < 3; i++ {
		select {
		case permit := <-got:
			permit.Release()
		case <-time.After(2 * time.Second):
			t.Fatal("queued Acquire never returned")
		}
	}
}

func TestStepReturnLagAndFloor(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{1}
	m := mustManager(t, src, opts)

	// Admission starts closed, so the first request queues until the first
	// step acquires budget: demand held(0)+1 -> target 2, held 2, then p1
	// is granted into the new capacity.
	got := make(chan *airlock.Permit, 1)
	go func() {
		permit, err := m.Acquire(context.Background(), "t", 1)
		if err == nil {
			got <- permit
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for m.Stats().Queued < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if m.Stats().Queued < 1 {
		t.Fatalf("queued = %d, want 1", m.Stats().Queued)
	}
	m.step()
	select {
	case p1 := <-got:
		defer p1.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("queued Acquire never returned")
	}
	p2, err := m.Acquire(context.Background(), "t", 1)
	if err != nil {
		t.Fatalf("Acquire p2: %v", err)
	}
	// Step 2: demand 2 -> target 3, held 3.
	m.step()
	p2.Release()
	// Step 3: the previous peak keeps demand 2 -> target 3, so nothing is low yet.
	m.step()
	if m.Held() != 3 {
		t.Fatalf("Held = %d, want 3 while peak holds the target", m.Held())
	}
	// Step 4: demand 1 -> target 2, first low-utilization step; LowPeriods == 2
	// so chunks are still held this step.
	m.step()
	if m.Held() != 3 {
		t.Fatalf("Held = %d, want 3 before LowPeriods elapses", m.Held())
	}
	// Step 5: third low step releases, stopping at the target.
	m.step()
	if m.Held() != 2 {
		t.Errorf("Held = %d, want 2 (returned down to target)", m.Held())
	}
	if got := len(src.weights()); got != 3 {
		t.Errorf("acquired %d chunks, want 3", got)
	}
}

func TestStepRenewFailureDropsChunk(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{4}
	opts.MinHeld = 4
	m := mustManager(t, src, opts)
	m.step()
	if m.Held() != 4 {
		t.Fatalf("Held = %d, want 4", m.Held())
	}
	// Drop the floor so the failed chunk is not immediately replaced.
	m.opts.MinHeld = 0
	src.mu.Lock()
	src.leases[0].renewErr = errors.New("lease lost")
	src.mu.Unlock()
	m.step()
	if m.Held() != 0 {
		t.Errorf("Held = %d, want 0 after renewal failure", m.Held())
	}
	if m.Stats().Capacity != 0 {
		t.Errorf("local capacity = %d, want 0", m.Stats().Capacity)
	}
}

func TestStepHeldZeroResizesLocalToZero(t *testing.T) {
	src := &fakeSource{}
	m := mustManager(t, src, baseOpts())
	m.step()
	if m.Stats().Capacity != 0 {
		t.Errorf("local capacity = %d, want 0", m.Stats().Capacity)
	}
	if _, err := m.TryAcquire("t", 1); !errors.Is(err, airlock.ErrWouldBlock) {
		t.Errorf("TryAcquire at zero budget: err = %v, want ErrWouldBlock", err)
	}
}

func TestStepPoolBlockedAndRecovery(t *testing.T) {
	src := &fakeSource{blocked: true}
	opts := baseOpts()
	opts.MinHeld = 32
	m := mustManager(t, src, opts)
	m.step()
	if m.Held() != 0 {
		t.Fatal("blocked pool granted budget")
	}
	m.step()
	if len(src.weights()) != 0 {
		t.Fatal("blocked retry granted budget")
	}
	src.setBlocked(false)
	m.step()
	if m.Held() != 32 {
		t.Fatalf("Held=%d, want32 after recovery", m.Held())
	}
}

func TestStepPoolCapacityAndRecovery(t *testing.T) {
	src := &fakeSource{capacity: 16}
	opts := baseOpts()
	opts.MinHeld = 32
	m := mustManager(t, src, opts)
	m.step()
	if m.Held() != 16 {
		t.Fatalf("Held=%d want16", m.Held())
	}
	src.mu.Lock()
	src.capacity = 0
	src.mu.Unlock()
	m.step()
	if m.Held() != 32 {
		t.Fatalf("Held=%d want32 after recovery", m.Held())
	}
}

func TestStepBackendErrorKeepsState(t *testing.T) {
	src := &fakeSource{}
	src.mu.Lock()
	src.err = errors.New("backend unreachable")
	src.mu.Unlock()
	opts := baseOpts()
	opts.Denominations = []int{4}
	opts.MinHeld = 4
	m := mustManager(t, src, opts)
	m.step()
	if m.Held() != 0 {
		t.Errorf("Held = %d, want 0", m.Held())
	}
	// Recovery on the next step still works.
	src.mu.Lock()
	src.err = nil
	src.mu.Unlock()
	m.step()
	if m.Held() != 4 {
		t.Errorf("Held = %d, want 4 after backend recovery", m.Held())
	}
}

func TestAcquireTryAcquireHeldStats(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{1}
	opts.MinHeld = 1
	m := mustManager(t, src, opts)
	m.step()
	if m.Held() != 1 {
		t.Fatalf("Held = %d, want 1", m.Held())
	}
	p, err := m.Acquire(context.Background(), "t", 1)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if s := m.Stats(); s.InUse != 1 || s.Capacity != 1 {
		t.Errorf("Stats = %+v, want InUse 1 Capacity 1", s)
	}
	if _, err := m.TryAcquire("t", 1); !errors.Is(err, airlock.ErrWouldBlock) {
		t.Errorf("TryAcquire when full: err = %v, want ErrWouldBlock", err)
	}
	p.Release()
	q, err := m.TryAcquire("t", 1)
	if err != nil {
		t.Fatalf("TryAcquire after release: %v", err)
	}
	q.Release()
}

func TestCloseReleasesAllAndIdempotent(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{4}
	opts.MinHeld = 8
	m := mustManager(t, src, opts)
	m.step()
	if len(m.chunks) != 2 {
		t.Fatalf("held chunks = %d, want 2", len(m.chunks))
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if m.Held() != 0 {
		t.Errorf("Held = %d, want 0 after Close", m.Held())
	}
	src.mu.Lock()
	for i, l := range src.leases {
		if l.released != 1 {
			t.Errorf("lease %d released %d times, want 1", i, l.released)
		}
	}
	src.mu.Unlock()
	if err := m.Close(context.Background()); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}
}

func TestCloseReportsReleaseErrors(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.Denominations = []int{4}
	opts.MinHeld = 4
	m := mustManager(t, src, opts)
	m.step()
	src.mu.Lock()
	src.leases[0].releaseErr = errors.New("release failed")
	src.mu.Unlock()
	if err := m.Close(context.Background()); err == nil {
		t.Error("Close with failing release: want error")
	}
	// Unknown cleanup remains frozen and may be retried with the same lease.
	src.leases[0].releaseErr = nil
	if err := m.Close(context.Background()); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}
}

// grantDriver is a minimal in-memory airlock.Driver that always grants, used
// to exercise the real New/sharedSource/sharedChunk path.
type grantDriver struct {
	mu     sync.Mutex
	leases map[string]string
	rev    uint64
}

func (d *grantDriver) next() uint64 { d.rev++; return d.rev }

func (d *grantDriver) Submit(ctx context.Context, req airlock.Request) (airlock.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id := fmt.Sprintf("lease-%d", d.next())
	if d.leases == nil {
		d.leases = map[string]string{}
	}
	d.leases[req.ID] = id
	return airlock.Result{Status: airlock.Granted, LeaseID: id, Weight: req.Weight, Remaining: req.Config.LeaseTTL, Revision: d.rev}, nil
}

func (d *grantDriver) Observe(ctx context.Context, req airlock.Request) (airlock.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id, ok := d.leases[req.ID]; ok {
		return airlock.Result{Status: airlock.Granted, LeaseID: id, Weight: req.Weight, Remaining: req.Config.LeaseTTL, Revision: d.rev}, nil
	}
	return airlock.Result{Status: airlock.Expired}, nil
}

func (d *grantDriver) Cancel(ctx context.Context, req airlock.Request) (airlock.Result, error) {
	return airlock.Result{Status: airlock.Cancelled}, nil
}

func (d *grantDriver) Renew(ctx context.Context, ref airlock.LeaseRef) (airlock.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id, ok := d.leases[ref.Request.ID]; ok && id == ref.ID {
		return airlock.Result{Status: airlock.Granted, LeaseID: id, Weight: ref.Request.Weight, Remaining: ref.Request.Config.LeaseTTL, Revision: d.rev}, nil
	}
	return airlock.Result{Status: airlock.Expired}, airlock.ErrLeaseLost
}

func (d *grantDriver) Release(ctx context.Context, ref airlock.LeaseRef) (airlock.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.leases, ref.Request.ID)
	return airlock.Result{Status: airlock.Released, Weight: ref.Request.Weight, Revision: d.rev}, nil
}

func TestNewRunsControlLoop(t *testing.T) {
	// Drive the real New + sharedSource path with a short interval; the
	// background loop must acquire chunks without manual step calls.
	m, err := New(Options{
		Driver:    &grantDriver{},
		Resource:  "res",
		GatewayID: "gw",
		Policy: airlock.ResourceConfig{
			Admission:    airlock.Config{Capacity: 8, MaxQueued: 8},
			LeaseTTL:     5 * time.Second,
			SubmitWindow: time.Second,
			Retention:    time.Minute,
			MaxRecords:   100,
		},
		Interval:         2 * time.Millisecond,
		Denominations:    []int{4},
		MinHeld:          4,
		OperationTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for m.Held() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if m.Held() != 4 {
		t.Fatalf("Held = %d, want 4 acquired by the background loop", m.Held())
	}
	if err := m.Close(context.Background()); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestNewWithSharedSource(t *testing.T) {
	opts := Options{
		Driver:           &grantDriver{},
		Resource:         "res",
		GatewayID:        "gw",
		Policy:           airlock.ResourceConfig{Admission: airlock.Config{Capacity: 64, MaxQueued: 8}, LeaseTTL: 5 * time.Second, MaxWait: 50 * time.Millisecond, SubmitWindow: time.Second, Retention: time.Minute, MaxRecords: 100},
		Interval:         time.Hour, // keep the loop goroutine quiet
		Denominations:    []int{4},
		MinHeld:          4,
		Headroom:         1.3,
		LowUtilization:   0.5,
		LowPeriods:       2,
		LocalQueue:       8,
		OperationTimeout: 500 * time.Millisecond,
	}
	m, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = m.Close(context.Background()) }()

	// Two manual steps: the first acquires a chunk, the second renews it.
	m.step()
	if m.Held() != 4 {
		t.Errorf("Held = %d, want 4", m.Held())
	}
	m.step()
	if m.Held() != 4 {
		t.Errorf("Held = %d, want 4 after renewal", m.Held())
	}
	if s := m.Stats(); s.Capacity != 4 {
		t.Errorf("local capacity = %d, want 4", s.Capacity)
	}
	if err := m.Close(context.Background()); err != nil {
		t.Errorf("Close: %v", err)
	}
	if m.Held() != 0 {
		t.Errorf("Held = %d, want 0 after Close", m.Held())
	}
}

// Package budget assembles a stage-1 hierarchical budget prototype on top of
// ordinary airlock primitives: discrete-denomination chunk leases from a shared
// budget pool back a local Airlock that performs per-request admission, while a
// control loop grows and shrinks the held chunk set from demand estimates.
//
// It is experimental reference code for the stage-one portion of
// docs/proposals/004-hierarchical-budget-allocation.md and carries no public
// API stability promise.
package budget

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/tr1v3r/airlock"
)

// Options configures a Manager. Zero optional fields select the documented
// defaults; required fields are Driver, Resource, GatewayID and a Policy with a
// positive Admission.Capacity.
type Options struct {
	// Driver is the budget pool backend handed to airlock.NewShared.
	Driver airlock.Driver
	// Resource is the pool resource name shared by all gateways.
	Resource string
	// GatewayID is this gateway's identity, used as the Shared tenant.
	GatewayID string
	// Policy is the pool policy; all clients must use the same resolved policy.
	Policy airlock.ResourceConfig
	// Interval is the control loop period; zero defaults to 200ms.
	Interval time.Duration
	// Denominations lists chunk weights; zero defaults to {16,4,1}.
	// Values are copied and sorted descending; all must be positive.
	Denominations []int
	// MaxChunks caps the number of simultaneously held chunks; zero defaults to 12.
	MaxChunks int
	// MinHeld is the lower clamp for the held weight target.
	MinHeld int
	// MaxHeld is the upper clamp; zero defaults to Policy.Admission.Capacity.
	MaxHeld int
	// Headroom scales demand into the target; zero defaults to 1.3.
	Headroom float64
	// LowUtilization is the InUse/held ratio below which chunks are returned;
	// zero defaults to 0.5.
	LowUtilization float64
	// LowPeriods is the number of consecutive low-utilization steps before
	// returning chunks; zero defaults to 2.
	LowPeriods int
	// LocalQueue is the local waiting queue bound; zero defaults to 8.
	LocalQueue int
	// OperationTimeout bounds each chunk operation context; zero defaults to 500ms.
	OperationTimeout time.Duration
}

// chunkLease is one held chunk of budget. A Renew error permanently invalidates
// the underlying Shared lease; the Manager must drop it instead of renewing again.
type chunkLease interface {
	// Weight is the chunk's fixed denomination.
	Weight() int
	// Renew extends the chunk's validity, or invalidates it permanently on error.
	Renew(ctx context.Context) error
	// Release returns the whole chunk to the pool.
	Release(ctx context.Context) error
}

// chunkSource acquires whole chunks from the budget pool for a tenant.
type chunkSource interface {
	Acquire(ctx context.Context, tenant string, weight int) (chunkLease, error)
}

// defaultDenominations is the fallback denomination set, largest first.
var defaultDenominations = []int{16, 4, 1}

// Manager holds discrete-denomination budget chunks from a shared pool and
// fronts them with a local Airlock for per-request admission. A background
// control loop renews held chunks and resizes the holding toward a demand-based
// target. Use New; the zero value is not usable. A Manager is safe for
// concurrent use and must not be copied.
type Manager struct {
	// opts is the validated configuration with defaults filled.
	opts Options
	// src supplies new chunks.
	src chunkSource
	// local performs request-level admission against the held budget.
	local *airlock.Airlock

	// mu protects every mutable field below.
	mu sync.Mutex
	// chunks is the set of currently held leases.
	chunks []chunkLease
	// paused records that the pool rejected a chunk as too heavy.
	paused bool
	// lowStreak counts consecutive low-utilization steps.
	lowStreak int
	// intervalPeak is the InUse snapshot of the previous step.
	intervalPeak int
	// stop closes to end the control loop goroutine.
	stop chan struct{}
	// stopped records that stop was closed, guarding Close idempotency.
	stopped bool
}

// New validates opts, constructs the shared budget source, and starts the
// control loop goroutine, which calls step once per Interval. The caller must
// Close the Manager to stop it.
func New(opts Options) (*Manager, error) {
	if opts.Driver == nil {
		return nil, errors.New("budget: nil driver")
	}
	src, err := newSharedSource(opts)
	if err != nil {
		return nil, err
	}
	m, err := newManager(src, opts)
	if err != nil {
		return nil, err
	}
	go m.run()
	return m, nil
}

// newManager validates opts, fills defaults, and constructs a Manager around
// src without starting the control loop; step can then be driven manually.
func newManager(src chunkSource, opts Options) (*Manager, error) {
	if src == nil {
		return nil, errors.New("budget: nil chunk source")
	}
	if opts.Resource == "" {
		return nil, errors.New("budget: empty resource")
	}
	if opts.GatewayID == "" {
		return nil, errors.New("budget: empty gateway id")
	}
	if opts.Interval <= 0 {
		opts.Interval = 200 * time.Millisecond
	}
	if len(opts.Denominations) == 0 {
		opts.Denominations = defaultDenominations
	} else {
		denoms := make([]int, len(opts.Denominations))
		for i, d := range opts.Denominations {
			if d <= 0 {
				return nil, fmt.Errorf("budget: nonpositive denomination %d", d)
			}
			denoms[i] = d
		}
		sort.Sort(sort.Reverse(sort.IntSlice(denoms)))
		opts.Denominations = denoms
	}
	if opts.MaxChunks <= 0 {
		opts.MaxChunks = 12
	}
	if opts.MaxHeld == 0 {
		opts.MaxHeld = opts.Policy.Admission.Capacity
	}
	if opts.Headroom <= 0 {
		opts.Headroom = 1.3
	}
	if opts.LowUtilization <= 0 {
		opts.LowUtilization = 0.5
	}
	if opts.LowPeriods <= 0 {
		opts.LowPeriods = 2
	}
	if opts.LocalQueue <= 0 {
		opts.LocalQueue = 8
	}
	if opts.OperationTimeout <= 0 {
		opts.OperationTimeout = 500 * time.Millisecond
	}
	if opts.MaxHeld <= 0 {
		return nil, errors.New("budget: policy capacity must be positive when MaxHeld is unset")
	}
	if opts.MinHeld < 0 || opts.MinHeld > opts.MaxHeld {
		return nil, fmt.Errorf("budget: MinHeld %d outside [0, MaxHeld %d]", opts.MinHeld, opts.MaxHeld)
	}

	// The local Airlock requires a positive creation capacity; the control
	// loop's Resize(0) legitimately pauses admission later. Creation grants a
	// phantom slot that no chunk backs, so admission stays closed until the
	// first step resizes to a real holding.
	local, err := airlock.New(airlock.Config{
		Capacity:           max(opts.MinHeld, 1),
		MaxQueued:          opts.LocalQueue,
		MaxQueuedPerTenant: opts.LocalQueue,
	})
	if err != nil {
		return nil, err
	}
	if err := local.Resize(0); err != nil {
		return nil, err
	}
	return &Manager{
		opts:  opts,
		src:   src,
		local: local,
		stop:  make(chan struct{}),
	}, nil
}

// run drives step on a ticker until Close stops it.
func (m *Manager) run() {
	ticker := time.NewTicker(m.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.step()
		case <-m.stop:
			return
		}
	}
}

// Acquire delegates to the local Airlock, waiting under local queue limits for
// budget held by this Manager. A nil context panics.
func (m *Manager) Acquire(ctx context.Context, tenant string, weight int) (*airlock.Permit, error) {
	return m.local.AcquireNFor(ctx, tenant, weight)
}

// TryAcquire delegates to the local Airlock without waiting, returning
// airlock.ErrWouldBlock when immediate admission is impossible.
func (m *Manager) TryAcquire(tenant string, weight int) (*airlock.Permit, error) {
	return m.local.TryAcquireNFor(tenant, weight)
}

// Held returns the total weight of currently held chunks, snapshotted under the
// Manager mutex. Chunks whose lease expired or failed renewal are excluded.
func (m *Manager) Held() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heldLocked()
}

// Stats returns the local Airlock snapshot.
func (m *Manager) Stats() airlock.Stats {
	return m.local.Stats()
}

// Close stops the control loop and releases every held chunk best-effort
// within ctx. It is idempotent and safe for concurrent use.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil
	}
	m.stopped = true
	close(m.stop)
	chunks := m.chunks
	m.chunks = nil
	m.mu.Unlock()

	var errs []error
	for _, c := range chunks {
		if err := c.Release(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// heldLocked sums the weights of held chunks; m.mu must be held.
func (m *Manager) heldLocked() int {
	held := 0
	for _, c := range m.chunks {
		held += c.Weight()
	}
	return held
}

// denomination returns the largest denomination not exceeding want, or false.
func (m *Manager) denomination(want int) (int, bool) {
	for _, d := range m.opts.Denominations {
		if d <= want {
			return d, true
		}
	}
	return 0, false
}

// step runs one control iteration: renew held chunks, estimate demand, move the
// holding toward the target, probe a paused pool, and resize local admission.
// It returns without side effects once Close has stopped the Manager, so a
// ticker firing concurrently with Close cannot leak a freshly acquired chunk.
func (m *Manager) step() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}

	// 1. Local demand snapshot.
	stats := m.local.Stats()

	// 2. Renew every chunk; a renewal error permanently invalidates the lease,
	// so drop it from the holding.
	live := m.chunks[:0]
	for _, c := range m.chunks {
		ctx, cancel := context.WithTimeout(context.Background(), m.opts.OperationTimeout)
		err := c.Renew(ctx)
		cancel()
		if err != nil {
			continue
		}
		live = append(live, c)
	}
	m.chunks = live

	// 3. Demand estimate: weighted use with the previous interval's peak,
	// lifted by queue pressure to grow beyond current holdings.
	held := m.heldLocked()
	base := stats.InUse
	if m.intervalPeak > base {
		base = m.intervalPeak
	}
	demand := base
	if stats.Queued > 0 {
		demand = held + stats.Queued
	}

	// 4. Target with headroom, clamped to policy bounds.
	target := int(math.Ceil(float64(demand) * m.opts.Headroom))
	if target < m.opts.MinHeld {
		target = m.opts.MinHeld
	}
	if target > m.opts.MaxHeld {
		target = m.opts.MaxHeld
	}

	// 5. Acquire chunks toward the target. A pool rejection for weight pauses
	// acquisition until the probe in step 7 unblocks it.
	if !m.paused {
		for held < target && len(m.chunks) < m.opts.MaxChunks {
			d, ok := m.denomination(target - held)
			if !ok {
				break
			}
			lease, err := m.acquire(d)
			if err != nil {
				if errors.Is(err, airlock.ErrWeightExceedsCapacity) {
					m.paused = true
				}
				break
			}
			m.chunks = append(m.chunks, lease)
			held += d
		}
	}

	// 6. Return chunks only after LowPeriods consecutive low-utilization steps,
	// never below the target, and never stranding in-flight permits: a chunk is
	// returned only when the remaining holding still covers stats.InUse, so the
	// sum of local in-use across gateways stays within the pool capacity except
	// for pool-side lease expiry.
	if held > target && (held == 0 || float64(stats.InUse) <= float64(held)*m.opts.LowUtilization) {
		m.lowStreak++
		if m.lowStreak >= m.opts.LowPeriods {
			for len(m.chunks) > 0 {
				heaviest := 0
				for i, c := range m.chunks {
					if c.Weight() > m.chunks[heaviest].Weight() {
						heaviest = i
					}
				}
				if held-m.chunks[heaviest].Weight() < target || stats.InUse > held-m.chunks[heaviest].Weight() {
					break
				}
				c := m.chunks[heaviest]
				m.chunks = append(m.chunks[:heaviest], m.chunks[heaviest+1:]...)
				held -= c.Weight()
				ctx, cancel := context.WithTimeout(context.Background(), m.opts.OperationTimeout)
				_ = c.Release(ctx)
				cancel()
			}
			m.lowStreak = 0
		}
	} else {
		m.lowStreak = 0
	}

	// 7. While paused, probe with the smallest denomination to detect the pool
	// opening up again; success also counts toward the holding.
	if m.paused {
		d := m.opts.Denominations[len(m.opts.Denominations)-1]
		if lease, err := m.acquire(d); err == nil {
			m.paused = false
			m.chunks = append(m.chunks, lease)
			held += d
		}
	}

	// 8. Local admission follows the holding; zero pauses new admission while
	// queued requests and outstanding permits are preserved.
	_ = m.local.Resize(held)

	// 9. Remember this interval's use as the next step's peak.
	m.intervalPeak = stats.InUse
}

// acquire fetches one chunk from the source for this gateway; m.mu must be held.
func (m *Manager) acquire(weight int) (chunkLease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), m.opts.OperationTimeout)
	defer cancel()
	return m.src.Acquire(ctx, m.opts.GatewayID, weight)
}

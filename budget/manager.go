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
	// A nonzero value must be finite and at least one.
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
	// Valid reports the underlying lease's current conservative validity.
	Valid() bool
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
	// lowStreak counts consecutive low-utilization steps.
	lowStreak int
	// previousInUse is the InUse snapshot of the previous step.
	previousInUse int
	// stop closes to end the control loop goroutine.
	stop chan struct{}
	// controlGate serializes control iterations and shutdown without blocking mu on I/O.
	controlGate chan struct{}
	// lifetime cancels control RPCs and queued callers on shutdown.
	lifetime context.Context
	cancel   context.CancelFunc
	// pending registers each waiting request once, including its weight.
	pending map[*int]int
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
	if math.IsNaN(opts.Headroom) || math.IsInf(opts.Headroom, 0) || math.IsNaN(opts.LowUtilization) || math.IsInf(opts.LowUtilization, 0) {
		return nil, errors.New("budget: nonfinite demand tuning")
	}
	if opts.Headroom <= 0 {
		opts.Headroom = 1.3
	}
	if opts.Headroom < 1 {
		return nil, errors.New("budget: Headroom must be at least one")
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
	lifetime, cancel := context.WithCancel(context.Background())
	controlGate := make(chan struct{}, 1)
	controlGate <- struct{}{}
	return &Manager{
		opts:        opts,
		src:         src,
		local:       local,
		stop:        make(chan struct{}),
		controlGate: controlGate,
		lifetime:    lifetime,
		cancel:      cancel,
		pending:     make(map[*int]int),
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

// ErrClosed reports admission attempted after shutdown begins.
var ErrClosed = errors.New("budget: manager closed")

// Acquire waits in the bounded local queue for weight tokens. Temporary budget
// shortage is not an oversized request: only weights above MaxHeld are rejected.
// A waiting heavy request conservatively pauses dispatch until it fits; Airlock
// retains ownership of the actual tenant queues and protected-head selection.
func (m *Manager) Acquire(ctx context.Context, tenant string, weight int) (*airlock.Permit, error) {
	if ctx == nil {
		panic("budget: nil context")
	}
	if err := m.validateRequest(tenant, weight); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.resizeLocked()
	if m.stopped {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if len(m.pending) == 0 {
		if p, err := m.local.TryAcquireNFor(tenant, weight); err == nil {
			if m.local.Stats().InUse <= m.heldLocked() {
				m.mu.Unlock()
				return p, nil
			}
			p.Release()
		}
	}
	if err := ctx.Err(); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if len(m.pending) >= m.opts.LocalQueue {
		m.mu.Unlock()
		return nil, airlock.ErrQueueFull
	}
	key := new(int)
	m.pending[key] = weight
	m.resizeLocked()
	m.mu.Unlock()
	defer func() { m.mu.Lock(); delete(m.pending, key); m.resizeLocked(); m.mu.Unlock() }()
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(m.lifetime, cancel)
	defer stop()
	for {
		p, err := m.local.AcquireNFor(waitCtx, tenant, weight)
		m.mu.Lock()
		m.resizeLocked()
		if m.stopped {
			if p != nil {
				p.Release()
			}
			m.mu.Unlock()
			return nil, ErrClosed
		}
		if err != nil {
			m.mu.Unlock()
			// Shrinking can reject a queued request; its registered demand survives.
			if errors.Is(err, airlock.ErrWeightExceedsCapacity) {
				continue
			}
			return nil, err
		}
		if m.local.Stats().InUse <= m.heldLocked() {
			m.mu.Unlock()
			return p, nil
		}
		p.Release()
		m.mu.Unlock()
	}
}

// TryAcquire never queues and refuses to bypass a registered waiting request.
func (m *Manager) TryAcquire(tenant string, weight int) (*airlock.Permit, error) {
	if err := m.validateRequest(tenant, weight); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resizeLocked()
	if m.stopped {
		return nil, ErrClosed
	}
	if len(m.pending) > 0 || weight > m.heldLocked() {
		return nil, airlock.ErrWouldBlock
	}
	p, err := m.local.TryAcquireNFor(tenant, weight)
	if err == nil && m.local.Stats().InUse > m.heldLocked() {
		p.Release()
		m.resizeLocked()
		return nil, airlock.ErrWouldBlock
	}
	return p, err
}

// validateRequest distinguishes permanent policy limits from temporary holdings.
func (m *Manager) validateRequest(tenant string, weight int) error {
	if tenant == "" {
		return airlock.ErrInvalidTenant
	}
	if weight <= 0 {
		return airlock.ErrInvalidWeight
	}
	if weight > m.opts.MaxHeld {
		return airlock.ErrWeightExceedsCapacity
	}
	return nil
}

// Held returns confirmed currently valid chunk weight, excluding expired leases.
func (m *Manager) Held() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resizeLocked()
	return m.heldLocked()
}

// Stats returns the local snapshot; InUse includes outstanding permits even after
// their backing chunk expires. TTL does not stop application work in stage one.
func (m *Manager) Stats() airlock.Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resizeLocked()
	return m.local.Stats()
}

// resizeLocked synchronizes the local gate with valid budget and waiting weight.
// The Manager mutex must be held; this never calls the backend.
func (m *Manager) resizeLocked() {
	held := m.heldLocked()
	if m.stopped {
		held = 0
	} else {
		for _, weight := range m.pending {
			if weight > held {
				held = 0
				break
			}
		}
	}
	_ = m.local.Resize(held)
}

// Close stops new admission, cancels control I/O, then waits for the current
// control iteration and outstanding permits before returning chunks. A deadline
// leaves unreturned chunks charged and frozen; another Close may retry cleanup.
func (m *Manager) Close(ctx context.Context) error {
	if ctx == nil {
		panic("budget: nil context")
	}
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.stop)
		m.cancel()
	}
	m.resizeLocked()
	m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.controlGate:
	}
	defer func() { m.controlGate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		m.mu.Lock()
		inUse := m.local.Stats().InUse
		m.mu.Unlock()
		if inUse == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	m.mu.Lock()
	chunks := append([]chunkLease(nil), m.chunks...)
	m.mu.Unlock()
	var errs []error
	unreleasedChunks := make([]chunkLease, 0, len(chunks))
	for _, c := range chunks {
		err := ctx.Err()
		if err == nil {
			err = c.Release(ctx)
		}
		if err != nil {
			errs = append(errs, err)
			unreleasedChunks = append(unreleasedChunks, c)
		}
	}
	m.mu.Lock()
	m.chunks = unreleasedChunks
	m.mu.Unlock()
	return errors.Join(errs...)
}

// heldLocked sums only valid chunks; expiry never frees outstanding permits.
func (m *Manager) heldLocked() int {
	held := 0
	for _, c := range m.chunks {
		if c.Valid() {
			held += c.Weight()
		}
	}
	return held
}

// operationContext bounds one backend RPC and is cancelled by Close.
func (m *Manager) operationContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(m.lifetime, m.opts.OperationTimeout)
}

// step serializes control operations, but never holds the admission mutex during
// backend I/O. Acquired chunks are published only before shutdown begins.
func (m *Manager) step() {
	select {
	case <-m.lifetime.Done():
		return
	case <-m.controlGate:
	}
	defer func() { m.controlGate <- struct{}{} }()
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	chunks := append([]chunkLease(nil), m.chunks...)
	m.mu.Unlock()
	live := make([]chunkLease, 0, len(chunks))
	for _, c := range chunks {
		ctx, cancel := m.operationContext()
		err := c.Renew(ctx)
		cancel()
		if err == nil && c.Valid() {
			live = append(live, c)
		}
	}
	m.mu.Lock()
	if m.stopped {
		m.resizeLocked()
		m.mu.Unlock()
		return
	}
	m.chunks = live
	m.resizeLocked()
	stats := m.local.Stats()
	held := m.heldLocked()
	demand := max(stats.InUse, m.previousInUse)
	// Saturate before adding so arbitrary weights cannot overflow the estimate.
	for _, weight := range m.pending {
		if weight >= m.opts.MaxHeld-demand {
			demand = m.opts.MaxHeld
			break
		}
		demand += weight
	}
	target := m.opts.MaxHeld
	scaled := math.Ceil(float64(demand) * m.opts.Headroom)
	if scaled < float64(m.opts.MaxHeld) {
		target = int(scaled)
	}
	target = max(target, m.opts.MinHeld)
	m.previousInUse = stats.InUse
	m.mu.Unlock()

	// A failed larger denomination may leave useful smaller budget available.
	// Bound the whole grow pass; ordinary backend failures do not trigger probes.
	growCtx, cancelGrow := m.operationContext()
	defer cancelGrow()
	for held < target {
		m.mu.Lock()
		full := m.stopped || len(m.chunks) >= m.opts.MaxChunks
		m.mu.Unlock()
		if full {
			break
		}
		acquired := false
		for _, d := range m.opts.Denominations {
			if d > target-held {
				continue
			}
			lease, err := m.src.Acquire(growCtx, m.opts.GatewayID, d)
			if err != nil {
				if growCtx.Err() != nil {
					break
				}
				if errors.Is(err, airlock.ErrWeightExceedsCapacity) || errors.Is(err, airlock.ErrLeaseLost) || errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				break
			}
			m.mu.Lock()
			m.chunks = append(m.chunks, lease)
			held = m.heldLocked()
			m.resizeLocked()
			stopped := m.stopped
			m.mu.Unlock()
			acquired = true
			if stopped {
				return
			}
			break
		}
		if !acquired {
			break
		}
	}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	held = m.heldLocked()
	stats = m.local.Stats()
	if held > target && float64(stats.InUse) <= float64(held)*m.opts.LowUtilization {
		m.lowStreak++
	} else {
		m.lowStreak = 0
	}
	shrink := m.lowStreak >= m.opts.LowPeriods
	if shrink {
		m.lowStreak = 0
	}
	m.mu.Unlock()
	if !shrink {
		return
	}
	for {
		m.mu.Lock()
		held = m.heldLocked()
		candidate := -1
		for i, c := range m.chunks {
			if c.Valid() && held-c.Weight() >= target && (candidate < 0 || c.Weight() > m.chunks[candidate].Weight()) {
				candidate = i
			}
		}
		if candidate < 0 || m.stopped {
			m.mu.Unlock()
			return
		}
		c := m.chunks[candidate]
		remaining := held - c.Weight()
		// Freeze the donated weight first, then inspect occupancy under that gate.
		_ = m.local.Resize(remaining)
		if m.local.Stats().InUse > remaining {
			m.resizeLocked()
			m.mu.Unlock()
			return
		}
		m.chunks = append(m.chunks[:candidate], m.chunks[candidate+1:]...)
		m.resizeLocked()
		m.mu.Unlock()
		ctx, cancel := m.operationContext()
		_ = c.Release(ctx)
		cancel()
	}
}

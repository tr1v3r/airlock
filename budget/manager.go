// Package budget assembles a stage-1 hierarchical budget prototype on top of
// ordinary airlock primitives: discrete-denomination chunk leases from a shared
// budget pool back a local Airlock that performs per-request admission, while a
// controlGate loop grows and shrinks the held chunk set from demand estimates.
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
// controlGate loop renews held chunks and resizes the holding toward a demand-based
// target. Use New; the zero value is not usable. A Manager is safe for
// concurrent use and must not be copied.
type Manager struct {
	// options is the validated configuration with defaults filled.
	options Options
	// source supplies new chunks.
	source chunkSource
	// admission performs request-level admission against the held budget.
	admission *airlock.Airlock

	// mu protects every mutable field below.
	mu sync.Mutex
	// heldChunks is the set of currently held leases.
	heldChunks []chunkLease
	// lowStreak counts consecutive low-utilization steps.
	lowStreak int
	// previousInUse is the InUse snapshot of the previous step.
	previousInUse int
	// shutdown closes to end the control loop goroutine.
	shutdown chan struct{}
	// controlGate serializes control iterations and shutdown without blocking mu on I/O.
	controlGate chan struct{}
	// lifecycleCtx cancels control RPCs and queued callers on shutdown.
	lifecycleCtx    context.Context
	cancelLifecycle context.CancelFunc
	// pendingWeights registers each waiting request once, including its weight.
	pendingWeights map[*int]int
	// closing stops new admission while shutdown and cleanup are in progress.
	closing bool
}

// New validates options, constructs the shared budget source, and starts the
// controlGate loop goroutine, which calls step once per Interval. The caller must
// Close the Manager to stop it.
func New(options Options) (*Manager, error) {
	if options.Driver == nil {
		return nil, errors.New("budget: nil driver")
	}
	source, err := newSharedSource(options)
	if err != nil {
		return nil, err
	}
	m, err := newManager(source, options)
	if err != nil {
		return nil, err
	}
	go m.run()
	return m, nil
}

// newManager validates options, fills defaults, and constructs a Manager around
// source without starting the control loop; step can then be driven manually.
func newManager(source chunkSource, options Options) (*Manager, error) {
	if source == nil {
		return nil, errors.New("budget: nil chunk source")
	}
	if options.Resource == "" {
		return nil, errors.New("budget: empty resource")
	}
	if options.GatewayID == "" {
		return nil, errors.New("budget: empty gateway id")
	}
	if options.Interval <= 0 {
		options.Interval = 200 * time.Millisecond
	}
	if len(options.Denominations) == 0 {
		options.Denominations = defaultDenominations
	} else {
		denoms := make([]int, len(options.Denominations))
		for i, chunkWeight := range options.Denominations {
			if chunkWeight <= 0 {
				return nil, fmt.Errorf("budget: nonpositive denomination %d", chunkWeight)
			}
			denoms[i] = chunkWeight
		}
		sort.Sort(sort.Reverse(sort.IntSlice(denoms)))
		options.Denominations = denoms
	}
	if options.MaxChunks <= 0 {
		options.MaxChunks = 12
	}
	if options.MaxHeld == 0 {
		options.MaxHeld = options.Policy.Admission.Capacity
	}
	if math.IsNaN(options.Headroom) || math.IsInf(options.Headroom, 0) || math.IsNaN(options.LowUtilization) || math.IsInf(options.LowUtilization, 0) {
		return nil, errors.New("budget: nonfinite demand tuning")
	}
	if options.Headroom <= 0 {
		options.Headroom = 1.3
	}
	if options.Headroom < 1 {
		return nil, errors.New("budget: Headroom must be at least one")
	}
	if options.LowUtilization <= 0 {
		options.LowUtilization = 0.5
	}
	if options.LowPeriods <= 0 {
		options.LowPeriods = 2
	}
	if options.LocalQueue <= 0 {
		options.LocalQueue = 8
	}
	if options.OperationTimeout <= 0 {
		options.OperationTimeout = 500 * time.Millisecond
	}
	if options.MaxHeld <= 0 {
		return nil, errors.New("budget: policy capacity must be positive when MaxHeld is unset")
	}
	if options.MinHeld < 0 || options.MinHeld > options.MaxHeld {
		return nil, fmt.Errorf("budget: MinHeld %d outside [0, MaxHeld %d]", options.MinHeld, options.MaxHeld)
	}

	// The local Airlock requires a positive creation capacity; the control
	// loop's Resize(0) legitimately pauses admission later. Creation grants a
	// phantom slot that no chunk backs, so admission stays closed until the
	// first step resizes to a real holding.
	admission, err := airlock.New(airlock.Config{
		Capacity:           max(options.MinHeld, 1),
		MaxQueued:          options.LocalQueue,
		MaxQueuedPerTenant: options.LocalQueue,
	})
	if err != nil {
		return nil, err
	}
	if err := admission.Resize(0); err != nil {
		return nil, err
	}
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	controlGate := make(chan struct{}, 1)
	controlGate <- struct{}{}
	return &Manager{
		options:         options,
		source:          source,
		admission:       admission,
		shutdown:        make(chan struct{}),
		controlGate:     controlGate,
		lifecycleCtx:    lifecycleCtx,
		cancelLifecycle: cancelLifecycle,
		pendingWeights:  make(map[*int]int),
	}, nil
}

// run drives step on a ticker until Close stops it.
func (m *Manager) run() {
	ticker := time.NewTicker(m.options.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.step()
		case <-m.shutdown:
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
	if m.closing {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if len(m.pendingWeights) == 0 {
		if permit, err := m.admission.TryAcquireNFor(tenant, weight); err == nil {
			if m.admission.Stats().InUse <= m.heldLocked() {
				m.mu.Unlock()
				return permit, nil
			}
			permit.Release()
		}
	}
	if err := ctx.Err(); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if len(m.pendingWeights) >= m.options.LocalQueue {
		m.mu.Unlock()
		return nil, airlock.ErrQueueFull
	}
	waiterID := new(int)
	m.pendingWeights[waiterID] = weight
	m.resizeLocked()
	m.mu.Unlock()
	defer func() { m.mu.Lock(); delete(m.pendingWeights, waiterID); m.resizeLocked(); m.mu.Unlock() }()
	acquireCtx, cancelAcquire := context.WithCancel(ctx)
	defer cancelAcquire()
	stopShutdownHook := context.AfterFunc(m.lifecycleCtx, cancelAcquire)
	defer stopShutdownHook()
	for {
		permit, err := m.admission.AcquireNFor(acquireCtx, tenant, weight)
		m.mu.Lock()
		m.resizeLocked()
		if m.closing {
			if permit != nil {
				permit.Release()
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
		if m.admission.Stats().InUse <= m.heldLocked() {
			m.mu.Unlock()
			return permit, nil
		}
		permit.Release()
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
	if m.closing {
		return nil, ErrClosed
	}
	if len(m.pendingWeights) > 0 || weight > m.heldLocked() {
		return nil, airlock.ErrWouldBlock
	}
	permit, err := m.admission.TryAcquireNFor(tenant, weight)
	if err == nil && m.admission.Stats().InUse > m.heldLocked() {
		permit.Release()
		m.resizeLocked()
		return nil, airlock.ErrWouldBlock
	}
	return permit, err
}

// validateRequest distinguishes permanent policy limits from temporary holdings.
func (m *Manager) validateRequest(tenant string, weight int) error {
	if tenant == "" {
		return airlock.ErrInvalidTenant
	}
	if weight <= 0 {
		return airlock.ErrInvalidWeight
	}
	if weight > m.options.MaxHeld {
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
	return m.admission.Stats()
}

// resizeLocked synchronizes the local gate with valid budget and waiting weight.
// The Manager mutex must be held; this never calls the backend.
func (m *Manager) resizeLocked() {
	heldWeight := m.heldLocked()
	if m.closing {
		heldWeight = 0
	} else {
		for _, weight := range m.pendingWeights {
			if weight > heldWeight {
				heldWeight = 0
				break
			}
		}
	}
	_ = m.admission.Resize(heldWeight)
}

// Close stops new admission, cancels control I/O, then waits for the current
// controlGate iteration and outstanding permits before returning chunks. A deadline
// leaves unreturned chunks charged and frozen; another Close may retry cleanup.
func (m *Manager) Close(ctx context.Context) error {
	if ctx == nil {
		panic("budget: nil context")
	}
	m.mu.Lock()
	if !m.closing {
		m.closing = true
		close(m.shutdown)
		m.cancelLifecycle()
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
		inUse := m.admission.Stats().InUse
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
	heldChunks := append([]chunkLease(nil), m.heldChunks...)
	m.mu.Unlock()
	var errs []error
	unreleasedChunks := make([]chunkLease, 0, len(heldChunks))
	for _, chunk := range heldChunks {
		err := ctx.Err()
		if err == nil {
			err = chunk.Release(ctx)
		}
		if err != nil {
			errs = append(errs, err)
			unreleasedChunks = append(unreleasedChunks, chunk)
		}
	}
	m.mu.Lock()
	m.heldChunks = unreleasedChunks
	m.mu.Unlock()
	return errors.Join(errs...)
}

// heldLocked sums only valid chunks; expiry never frees outstanding permits.
func (m *Manager) heldLocked() int {
	heldWeight := 0
	for _, chunk := range m.heldChunks {
		if chunk.Valid() {
			heldWeight += chunk.Weight()
		}
	}
	return heldWeight
}

// operationContext bounds one backend RPC and is cancelled by Close.
func (m *Manager) operationContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(m.lifecycleCtx, m.options.OperationTimeout)
}

// step serializes control operations, but never holds the admission mutex during
// backend I/O. Acquired chunks are published only before shutdown begins.
func (m *Manager) step() {
	select {
	case <-m.lifecycleCtx.Done():
		return
	case <-m.controlGate:
	}
	defer func() { m.controlGate <- struct{}{} }()
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return
	}
	heldChunks := append([]chunkLease(nil), m.heldChunks...)
	m.mu.Unlock()
	renewedChunks := make([]chunkLease, 0, len(heldChunks))
	for _, chunk := range heldChunks {
		ctx, cancelOperation := m.operationContext()
		err := chunk.Renew(ctx)
		cancelOperation()
		if err == nil && chunk.Valid() {
			renewedChunks = append(renewedChunks, chunk)
		}
	}
	m.mu.Lock()
	if m.closing {
		m.resizeLocked()
		m.mu.Unlock()
		return
	}
	m.heldChunks = renewedChunks
	m.resizeLocked()
	stats := m.admission.Stats()
	heldWeight := m.heldLocked()
	demandWeight := max(stats.InUse, m.previousInUse)
	// Saturate before adding so arbitrary weights cannot overflow the estimate.
	for _, weight := range m.pendingWeights {
		if weight >= m.options.MaxHeld-demandWeight {
			demandWeight = m.options.MaxHeld
			break
		}
		demandWeight += weight
	}
	targetWeight := m.options.MaxHeld
	scaledDemand := math.Ceil(float64(demandWeight) * m.options.Headroom)
	if scaledDemand < float64(m.options.MaxHeld) {
		targetWeight = int(scaledDemand)
	}
	targetWeight = max(targetWeight, m.options.MinHeld)
	m.previousInUse = stats.InUse
	m.mu.Unlock()

	// A failed larger denomination may leave useful smaller budget available.
	// Bound the whole grow pass; ordinary backend failures do not trigger probes.
	growCtx, cancelGrow := m.operationContext()
	defer cancelGrow()
	for heldWeight < targetWeight {
		m.mu.Lock()
		cannotGrow := m.closing || len(m.heldChunks) >= m.options.MaxChunks
		m.mu.Unlock()
		if cannotGrow {
			break
		}
		chunkAcquired := false
		for _, chunkWeight := range m.options.Denominations {
			if chunkWeight > targetWeight-heldWeight {
				continue
			}
			lease, err := m.source.Acquire(growCtx, m.options.GatewayID, chunkWeight)
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
			m.heldChunks = append(m.heldChunks, lease)
			heldWeight = m.heldLocked()
			m.resizeLocked()
			closing := m.closing
			m.mu.Unlock()
			chunkAcquired = true
			if closing {
				return
			}
			break
		}
		if !chunkAcquired {
			break
		}
	}
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return
	}
	heldWeight = m.heldLocked()
	stats = m.admission.Stats()
	if heldWeight > targetWeight && float64(stats.InUse) <= float64(heldWeight)*m.options.LowUtilization {
		m.lowStreak++
	} else {
		m.lowStreak = 0
	}
	shouldReturn := m.lowStreak >= m.options.LowPeriods
	if shouldReturn {
		m.lowStreak = 0
	}
	m.mu.Unlock()
	if !shouldReturn {
		return
	}
	for {
		m.mu.Lock()
		heldWeight = m.heldLocked()
		returnIndex := -1
		for i, chunk := range m.heldChunks {
			if chunk.Valid() && heldWeight-chunk.Weight() >= targetWeight && (returnIndex < 0 || chunk.Weight() > m.heldChunks[returnIndex].Weight()) {
				returnIndex = i
			}
		}
		if returnIndex < 0 || m.closing {
			m.mu.Unlock()
			return
		}
		chunk := m.heldChunks[returnIndex]
		remainingWeight := heldWeight - chunk.Weight()
		// Freeze the donated weight first, then inspect occupancy under that gate.
		_ = m.admission.Resize(remainingWeight)
		if m.admission.Stats().InUse > remainingWeight {
			m.resizeLocked()
			m.mu.Unlock()
			return
		}
		m.heldChunks = append(m.heldChunks[:returnIndex], m.heldChunks[returnIndex+1:]...)
		m.resizeLocked()
		m.mu.Unlock()
		ctx, cancelOperation := m.operationContext()
		_ = chunk.Release(ctx)
		cancelOperation()
	}
}

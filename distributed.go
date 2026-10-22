package airlock

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// SharedOptions specifies one backend resource and bounded network polling.
// NewShared fills zero protocol fields with the defaults documented below.
// Resource and Config.Admission.Capacity are required. Admission queue limits
// remain unchanged: zero disables waiting. There is no implicit renewal or
// background acquisition worker.
type SharedOptions struct {
	Resource string
	// Config fixes resource-wide admission and lease policy. Zero protocol fields
	// default to LeaseTTL=30s, MaxWait=30s, SubmitWindow=5s, Retention=1m, and
	// MaxRecords=10000. All clients of a resource must use the same resolved Config.
	Config ResourceConfig
	// OperationTimeout bounds each backend call; zero defaults to 5s.
	OperationTimeout time.Duration
	// PollInterval spaces observations while queued; zero defaults to 50ms.
	PollInterval time.Duration
	// ClockSkewAllowance reserves time for a known bound on client/backend clock
	// offset. Zero preserves the original deadlines (no skew allowance). Positive
	// values must be whole milliseconds and less than half both SubmitWindow and
	// MaxWait. This client-local setting shortens submission and waiting deadlines,
	// including an earlier caller deadline; it does not extend lease validity.
	ClockSkewAllowance time.Duration
}

// Shared coordinates admission to a resource through an injected Driver.
// The driver determines the sharing scope. Grants are time-bounded leases.
// Shared never closes the driver or its connections.
// An acquisition context only controls waiting; delivered leases have their own
// lifetime. Backend failures never fall back to local admission.
type Shared struct {
	driver  Driver
	options SharedOptions
}

// NewShared creates shared admission through a driver and snapshots its options.
// Construction performs no backend I/O and acquires no tokens. The caller owns
// the driver and its connections; Acquire starts admission.
func NewShared(d Driver, options SharedOptions) (*Shared, error) {
	options = options.withDefaults()
	if d == nil || options.Resource == "" || !validText(options.Resource) || len(options.Resource) > 256 || options.OperationTimeout <= 0 || options.OperationTimeout > time.Minute || options.PollInterval <= 0 || options.PollInterval > time.Minute {
		return nil, ErrInvalidConfig
	}
	if err := validConfig(options.Config); err != nil {
		return nil, err
	}
	allowance := options.ClockSkewAllowance
	// Check each upper bound before subtraction; never multiply an untrusted
	// duration by two (which could overflow). Both windows need room for +/-skew.
	if allowance < 0 || allowance%time.Millisecond != 0 || allowance >= options.Config.SubmitWindow || allowance >= options.Config.MaxWait || allowance >= options.Config.SubmitWindow-allowance || allowance >= options.Config.MaxWait-allowance {
		return nil, ErrInvalidConfig
	}
	data, _ := json.Marshal(options)
	var copied SharedOptions
	_ = json.Unmarshal(data, &copied)
	return &Shared{driver: d, options: copied}, nil
}

// Acquire obtains one leased token for the default tenant.
func (d *Shared) Acquire(ctx context.Context) (*Lease, error) { return d.AcquireN(ctx, 1) }

// AcquireFor obtains one leased token for an explicitly named tenant.
func (d *Shared) AcquireFor(ctx context.Context, tenant string) (*Lease, error) {
	return d.AcquireNFor(ctx, tenant, 1)
}

// AcquireN obtains n leased tokens atomically for the default tenant.
func (d *Shared) AcquireN(ctx context.Context, n int) (*Lease, error) {
	return d.acquire(ctx, "", n)
}

// AcquireNFor obtains n leased tokens atomically for an explicitly named tenant.
func (d *Shared) AcquireNFor(ctx context.Context, tenant string, n int) (*Lease, error) {
	if ctx == nil {
		panic("airlock: nil context")
	}
	if tenant == "" {
		return nil, ErrInvalidTenant
	}
	return d.acquire(ctx, tenant, n)
}
func (d *Shared) execute(ctx context.Context, operation func(context.Context) (Result, error)) (Result, time.Time, error) {
	op, cancel := context.WithTimeout(ctx, d.options.OperationTimeout)
	defer cancel()
	start := time.Now()
	if err := op.Err(); err != nil {
		return Result{}, start, err
	}
	result, err := operation(op)
	// Even a driver that reports success after its I/O deadline cannot authorize work.
	if err == nil {
		err = op.Err()
	}
	return result, start, err
}
func (d *Shared) acquire(ctx context.Context, tenant string, n int) (*Lease, error) {
	if ctx == nil {
		panic("airlock: nil context")
	}
	if n <= 0 {
		return nil, ErrInvalidWeight
	}
	if n > d.options.Config.Admission.Capacity {
		return nil, ErrWeightExceedsCapacity
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now()
	allowance := d.options.ClockSkewAllowance
	r := NewRequest(now.Add(d.options.Config.SubmitWindow - allowance))
	r.Resource = d.options.Resource
	r.Config = d.options.Config
	r.Tenant = tenant
	r.Weight = n
	r.WaitUntil = now.Add(r.Config.MaxWait).UTC().Truncate(time.Millisecond)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(r.WaitUntil) {
		r.WaitUntil = deadline.UTC().Truncate(time.Millisecond)
	}
	if allowance > 0 {
		r.WaitUntil = r.WaitUntil.Add(-allowance)
		// A short caller deadline can consume the entire conservative waiting
		// budget. No command has been sent, so there is nothing to cancel.
		if !time.Now().Before(r.WaitUntil) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, ErrLeaseLost
		}
	}
	if err := validRequest(r); err != nil {
		return nil, err
	}
	operation := func(ctx context.Context) (Result, error) { return d.driver.Submit(ctx, r) }
	for {
		result, start, err := d.execute(ctx, operation)
		if err == nil {
			err = ctx.Err()
		}
		if err == nil {
			switch result.Status {
			case Granted:
				deadline := start.Add(result.Remaining)
				if result.LeaseID == "" || result.Weight != n || result.Remaining <= 0 || result.Remaining > r.Config.LeaseTTL || !time.Now().Before(deadline) {
					err = ErrLeaseLost
				} else {
					lease := &Lease{client: d, request: r, id: result.LeaseID, deadline: deadline, done: make(chan struct{})}
					lease.mu.Lock()
					lease.timer = time.AfterFunc(time.Until(deadline), lease.expire)
					lease.mu.Unlock()
					return lease, nil
				}
			case Queued:
				timer := time.NewTimer(d.options.PollInterval)
				select {
				case <-ctx.Done():
					timer.Stop()
					err = ctx.Err()
				case <-timer.C:
					operation = func(ctx context.Context) (Result, error) { return d.driver.Observe(ctx, r) }
					continue
				}
			default:
				err = ErrLeaseLost
			}
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.options.OperationTimeout)
		_, _, cleanupErr := d.execute(cleanup, func(ctx context.Context) (Result, error) { return d.driver.Cancel(ctx, r) })
		cancel()
		return nil, errors.Join(err, cleanupErr)
	}
}

// Lease owns a time-bounded whole allocation. Do not copy it. Callers must stop
// work when Done closes or Valid is false, and release only after work stops.
// Expiry reclaims accounting but cannot stop CPU work or fence external writes.
// Renew and Release are serialized; acquiring-context cancellation is unrelated.
type Lease struct {
	opMu     sync.Mutex
	mu       sync.Mutex
	client   *Shared
	request  Request
	id       string
	deadline time.Time
	timer    *time.Timer
	done     chan struct{}
	closed   bool
}

func (l *Lease) invalidateLocked() {
	if !l.closed {
		l.closed = true
		close(l.done)
		if l.timer != nil {
			l.timer.Stop()
		}
	}
}
func (l *Lease) expire() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !time.Now().Before(l.deadline) {
		l.invalidateLocked()
	}
}

// Done closes on expiry, uncertain renewal, or a release attempt. It never reopens.
func (l *Lease) Done() <-chan struct{} { return l.done }

// Valid checks the deadline directly, even if a paused process delayed its timer.
func (l *Lease) Valid() bool { l.expire(); l.mu.Lock(); defer l.mu.Unlock(); return !l.closed }

// Weight returns the atomic allocation size.
func (l *Lease) Weight() int { return l.request.Weight }

// Renew explicitly extends a live lease. Any error invalidates it permanently.
// A response after the previous local deadline never resurrects the lease.
func (l *Lease) Renew(ctx context.Context) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if !l.Valid() {
		return ErrLeaseLost
	}
	result, start, err := l.client.execute(ctx, func(ctx context.Context) (Result, error) {
		return l.client.driver.Renew(ctx, LeaseRef{Request: l.request, ID: l.id})
	})
	l.mu.Lock()
	defer l.mu.Unlock()
	deadline := start.Add(result.Remaining)
	if err == nil && (l.closed || !time.Now().Before(l.deadline) || result.Status != Granted || result.LeaseID != l.id || result.Weight != l.request.Weight || result.Remaining <= 0 || result.Remaining > l.request.Config.LeaseTTL || !time.Now().Before(deadline)) {
		err = ErrLeaseLost
	}
	if err != nil {
		l.invalidateLocked()
		return err
	}
	l.deadline = deadline
	l.timer.Stop()
	l.timer = time.AfterFunc(time.Until(deadline), l.expire)
	return nil
}

// Release invalidates locally before I/O. On errors the result is unknown; retry
// this same method for cleanup. Success means a matching terminal record exists.
func (l *Lease) Release(ctx context.Context) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	l.mu.Lock()
	l.invalidateLocked()
	l.mu.Unlock()
	result, _, err := l.client.execute(ctx, func(ctx context.Context) (Result, error) {
		return l.client.driver.Release(ctx, LeaseRef{Request: l.request, ID: l.id})
	})
	if err == nil && result.Status != Released && result.Status != Expired && result.Status != Cancelled {
		return ErrLeaseLost
	}
	return err
}

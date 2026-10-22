package airlock

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// DistributedOptions specifies one resource and bounded network polling.
// There is no implicit renewal or background acquisition worker.
type DistributedOptions struct {
	Resource         string
	Config           ResourceConfig
	OperationTimeout time.Duration
	PollInterval     time.Duration
}

// Distributed is a lease-based client. It never closes the injected Driver.
// An acquisition context only controls waiting; delivered leases have their own
// lifetime. Backend failures never fall back to local admission.
type Distributed struct {
	driver  Driver
	options DistributedOptions
}

// NewDistributed validates and snapshots all options without contacting a backend.
func NewDistributed(d Driver, options DistributedOptions) (*Distributed, error) {
	if d == nil || options.Resource == "" || len(options.Resource) > 256 || options.OperationTimeout <= 0 || options.OperationTimeout > time.Minute || options.PollInterval <= 0 || options.PollInterval > time.Minute {
		return nil, ErrInvalidConfig
	}
	if err := validConfig(options.Config); err != nil {
		return nil, err
	}
	data, _ := json.Marshal(options)
	var copied DistributedOptions
	_ = json.Unmarshal(data, &copied)
	return &Distributed{driver: d, options: copied}, nil
}

// Acquire obtains one leased token for the default tenant.
func (d *Distributed) Acquire(ctx context.Context) (*Lease, error) { return d.AcquireN(ctx, 1) }

// AcquireFor obtains one leased token for an explicitly named tenant.
func (d *Distributed) AcquireFor(ctx context.Context, tenant string) (*Lease, error) {
	return d.AcquireNFor(ctx, tenant, 1)
}

// AcquireN obtains n leased tokens atomically for the default tenant.
func (d *Distributed) AcquireN(ctx context.Context, n int) (*Lease, error) {
	return d.acquire(ctx, "", n)
}

// AcquireNFor obtains n leased tokens atomically for an explicitly named tenant.
func (d *Distributed) AcquireNFor(ctx context.Context, tenant string, n int) (*Lease, error) {
	if ctx == nil {
		panic("airlock: nil context")
	}
	if tenant == "" {
		return nil, ErrInvalidTenant
	}
	return d.acquire(ctx, tenant, n)
}
func (d *Distributed) execute(ctx context.Context, cmd Command) (Result, time.Time, error) {
	op, cancel := context.WithTimeout(ctx, d.options.OperationTimeout)
	defer cancel()
	start := time.Now()
	if err := op.Err(); err != nil {
		return Result{}, start, err
	}
	result, err := d.driver.Execute(op, cmd)
	// Even a driver that reports success after its I/O deadline cannot authorize work.
	if err == nil {
		err = op.Err()
	}
	return result, start, err
}
func (d *Distributed) acquire(ctx context.Context, tenant string, n int) (*Lease, error) {
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
	r := NewRequest(now.Add(d.options.Config.SubmitWindow))
	r.Resource = d.options.Resource
	r.Config = d.options.Config
	r.Tenant = tenant
	r.Weight = n
	r.WaitUntil = now.Add(r.Config.MaxWait).UTC().Truncate(time.Millisecond)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(r.WaitUntil) {
		r.WaitUntil = deadline.UTC().Truncate(time.Millisecond)
	}
	cmd := Command{Kind: Submit, Request: r}
	for {
		result, start, err := d.execute(ctx, cmd)
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
					cmd.Kind = Observe
					continue
				}
			default:
				err = ErrLeaseLost
			}
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.options.OperationTimeout)
		_, _, cleanupErr := d.execute(cleanup, Command{Kind: Cancel, Request: r})
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
	client   *Distributed
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
	result, start, err := l.client.execute(ctx, Command{Kind: Renew, Request: l.request, LeaseID: l.id})
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
	result, _, err := l.client.execute(ctx, Command{Kind: Release, Request: l.request, LeaseID: l.id})
	if err == nil && result.Status != Released && result.Status != Expired && result.Status != Cancelled {
		return ErrLeaseLost
	}
	return err
}

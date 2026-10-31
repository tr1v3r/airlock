package budget

import (
	"context"
	"time"

	"github.com/tr1v3r/airlock"
)

// sharedPollInterval and sharedMaxWait are the fixed Shared-layer waiting
// settings used by the budget source: chunk acquisition should give up quickly
// rather than hold the control loop. defaultOperationTimeout mirrors the
// Options.OperationTimeout default for constructing the Shared client before
// newManager normalizes the Options copy.
const (
	sharedPollInterval      = 20 * time.Millisecond
	sharedMaxWait           = 50 * time.Millisecond
	defaultOperationTimeout = 500 * time.Millisecond
)

// sharedSource acquires budget chunks through an airlock.Shared pool client.
type sharedSource struct {
	// shared is the pool client constructed from Options.
	shared *airlock.Shared
}

// newSharedSource builds the Shared pool client for opts. Policy protocol
// fields keep their explicit values; an unset MaxWait is shortened to keep
// control-loop acquisitions bounded.
func newSharedSource(opts Options) (*sharedSource, error) {
	cfg := opts.Policy
	if cfg.MaxWait == 0 {
		cfg.MaxWait = sharedMaxWait
	}

	// NewShared is constructed before newManager fills defaults in its own copy.
	timeout := opts.OperationTimeout
	if timeout <= 0 {
		timeout = defaultOperationTimeout
	}

	shared, err := airlock.NewShared(opts.Driver, airlock.SharedOptions{
		Resource:         opts.Resource,
		Config:           cfg,
		OperationTimeout: timeout,
		PollInterval:     sharedPollInterval,
	})
	if err != nil {
		return nil, err
	}

	return &sharedSource{shared: shared}, nil
}

// Acquire obtains a whole chunk of weight tokens for tenant through Shared
// waiting and lease-lifetime rules. Shared semantics apply: a lease whose
// Renew ever failed is permanently invalid and must be dropped by the caller.
func (s *sharedSource) Acquire(ctx context.Context, tenant string, weight int) (chunkLease, error) {
	lease, err := s.shared.AcquireNFor(ctx, tenant, weight)
	if err != nil {
		return nil, err
	}

	return &sharedChunk{lease: lease}, nil
}

// sharedChunk adapts an airlock.Lease to chunkLease by direct pass-through.
type sharedChunk struct {
	// lease is the underlying Shared lease.
	lease *airlock.Lease
}

// Weight returns the chunk denomination.
func (c *sharedChunk) Weight() int { return c.lease.Weight() }

// Renew extends the underlying lease; an error permanently invalidates it.
func (c *sharedChunk) Renew(ctx context.Context) error { return c.lease.Renew(ctx) }

// Release returns the whole chunk to the pool.
func (c *sharedChunk) Release(ctx context.Context) error { return c.lease.Release(ctx) }

// Valid observes the Shared lease deadline even between control iterations.
func (c *sharedChunk) Valid() bool { return c.lease.Valid() }

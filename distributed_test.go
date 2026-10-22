package airlock

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// clientStateDriver commits real protocol transitions before injecting response
// loss or latency. Its mutex models the backend's per-resource atomic boundary.
type clientStateDriver struct {
	mu    sync.Mutex
	state []byte
	calls []Command
	after func(context.Context, Command, Result, error) (Result, error)
}

func (d *clientStateDriver) Execute(ctx context.Context, cmd Command) (Result, error) {
	d.mu.Lock()
	data, result, err := Transition(d.state, time.Now(), cmd)
	if data != nil {
		d.state = data
	}
	d.calls = append(d.calls, cmd)
	d.mu.Unlock()
	if d.after != nil {
		return d.after(ctx, cmd, result, err)
	}
	return result, err
}
func (d *clientStateDriver) commands() []Command {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Command(nil), d.calls...)
}
func clientOptions() SharedOptions {
	return SharedOptions{Resource: "test", Config: ResourceConfig{Admission: Config{Capacity: 2, MaxQueued: 16, MaxQueuedPerTenant: 16}, LeaseTTL: time.Second, MaxWait: time.Second, SubmitWindow: time.Second, Retention: time.Second, MaxRecords: 100}, OperationTimeout: 500 * time.Millisecond, PollInterval: time.Millisecond}
}
func clientNew(t *testing.T, d Driver, options SharedOptions) *Shared {
	t.Helper()
	c, err := NewShared(d, options)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func clientAcquire(t *testing.T, c *Shared) *Lease {
	t.Helper()
	l, err := c.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Release(context.Background()) })
	return l
}
func clientAwait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}
func clientClosed(t *testing.T, l *Lease) {
	t.Helper()
	if l.Valid() {
		t.Fatal("lease still valid")
	}
	select {
	case <-l.Done():
	default:
		t.Fatal("Done remains open")
	}
}

func TestSharedOptionsValidationAndSnapshot(t *testing.T) {
	tests := []struct {
		name   string
		change func(*SharedOptions)
	}{
		{"empty resource", func(o *SharedOptions) { o.Resource = "" }},
		{"long resource", func(o *SharedOptions) { o.Resource = strings.Repeat("x", 257) }},
		{"zero timeout", func(o *SharedOptions) { o.OperationTimeout = 0 }},
		{"long timeout", func(o *SharedOptions) { o.OperationTimeout = time.Minute + 1 }},
		{"zero interval", func(o *SharedOptions) { o.PollInterval = 0 }},
		{"long interval", func(o *SharedOptions) { o.PollInterval = time.Minute + 1 }},
		{"invalid admission", func(o *SharedOptions) { o.Config.Admission.Capacity = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := clientOptions()
			tt.change(&o)
			if _, err := NewShared(&clientStateDriver{}, o); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error %v", err)
			}
		})
	}
	if _, err := NewShared(nil, clientOptions()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	o := clientOptions()
	o.Config.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 8}, {Name: "low", Turns: 1, MaxQueued: 8}}, DefaultGroup: "low", TenantGroups: map[string]string{"online": "high"}}
	c := clientNew(t, &clientStateDriver{}, o)
	o.Config.Admission.Priority.Groups[0].Turns = 100
	o.Config.Admission.Priority.TenantGroups["online"] = "low"
	if c.options.Config.Admission.Priority.Groups[0].Turns != 2 || c.options.Config.Admission.Priority.TenantGroups["online"] != "high" {
		t.Fatal("configuration not snapshotted")
	}
}

func TestSharedAcquireArguments(t *testing.T) {
	d := &clientStateDriver{}
	c := clientNew(t, d, clientOptions())
	for _, tc := range []struct {
		n   int
		err error
	}{{0, ErrInvalidWeight}, {-1, ErrInvalidWeight}, {3, ErrWeightExceedsCapacity}} {
		if _, err := c.AcquireN(context.Background(), tc.n); !errors.Is(err, tc.err) {
			t.Fatalf("weight %d: %v", tc.n, err)
		}
	}
	if _, err := c.AcquireFor(context.Background(), ""); !errors.Is(err, ErrInvalidTenant) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, f := range []func(){func() { _, _ = c.Acquire(nil) }, func() { _, _ = c.AcquireNFor(nil, "a", 1) }} { //nolint:staticcheck // SA1012: exercise the explicit nil-context panic contract.
		func() {
			defer func() {
				if recover() == nil {
					t.Error("nil context did not panic")
				}
			}()
			f()
		}()
	}
	if len(d.commands()) != 0 {
		t.Fatal("invalid requests contacted backend")
	}
}

func TestSharedLeaseLifecycle(t *testing.T) {
	d := &clientStateDriver{}
	c := clientNew(t, d, clientOptions())
	ctx, cancel := context.WithCancel(context.Background())
	l, err := c.AcquireNFor(ctx, "online", 2)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if !l.Valid() || l.Weight() != 2 {
		t.Fatal("acquisition context revoked delivered lease")
	}
	select {
	case <-l.Done():
		t.Fatal("done closed on acquisition cancellation")
	default:
	}
	old := l.deadline
	time.Sleep(3 * time.Millisecond)
	if err = l.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !l.deadline.After(old) {
		t.Fatal("renew did not extend lease")
	}
	if err = l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	clientClosed(t, l)
	if err = l.Release(context.Background()); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
	if err = l.Renew(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	l2, err := c.AcquireFor(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	if err = l2.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSharedQueuePollAndGrant(t *testing.T) {
	submitted := make(chan struct{})
	var once sync.Once
	d := &clientStateDriver{after: func(_ context.Context, c Command, r Result, e error) (Result, error) {
		if c.Kind == Submit && r.Status == Queued {
			once.Do(func() { close(submitted) })
		}
		return r, e
	}}
	o := clientOptions()
	o.Config.Admission.Capacity = 1
	c := clientNew(t, d, o)
	holder := clientAcquire(t, c)
	done := make(chan struct{})
	var acquired *Lease
	var err error
	go func() { acquired, err = c.AcquireFor(context.Background(), "queued"); close(done) }()
	clientAwait(t, submitted)
	if releaseErr := holder.Release(context.Background()); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	clientAwait(t, done)
	if err != nil {
		t.Fatal(err)
	}
	if err = acquired.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, cmd := range d.commands() {
		if cmd.Kind == Observe {
			seen = true
		}
	}
	if !seen {
		t.Fatal("queued request never observed")
	}
}

func TestSharedQueuedCancellationCleanupIndependentContext(t *testing.T) {
	queued := make(chan struct{})
	var once sync.Once
	var cleanupLive atomic.Bool
	d := &clientStateDriver{after: func(ctx context.Context, c Command, r Result, e error) (Result, error) {
		if c.Kind == Submit && r.Status == Queued {
			once.Do(func() { close(queued) })
		}
		if c.Kind == Cancel {
			cleanupLive.Store(ctx.Err() == nil)
		}
		return r, e
	}}
	o := clientOptions()
	o.Config.Admission.Capacity = 1
	o.PollInterval = time.Second
	c := clientNew(t, d, o)
	_ = clientAcquire(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	var err error
	go func() { _, err = c.Acquire(ctx); close(done) }()
	clientAwait(t, queued)
	cancel()
	clientAwait(t, done)
	if !errors.Is(err, context.Canceled) || !cleanupLive.Load() {
		t.Fatalf("error %v cleanupLive %v", err, cleanupLive.Load())
	}
	calls := d.commands()
	last := calls[len(calls)-1]
	if last.Kind != Cancel {
		t.Fatalf("last command %s", last.Kind)
	}
	d.mu.Lock()
	_, r, e := Transition(d.state, time.Now(), Command{Kind: Observe, Request: last.Request})
	d.mu.Unlock()
	if e != nil || r.Status != Cancelled {
		t.Fatalf("cancel did not terminate request: %v %v", r, e)
	}
}

func TestSharedDeadlinePropagatedAndNoAutomaticRenewal(t *testing.T) {
	d := &clientStateDriver{}
	o := clientOptions()
	o.Config.LeaseTTL = 40 * time.Millisecond
	c := clientNew(t, d, o)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	l, err := c.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	if !d.commands()[0].Request.WaitUntil.Equal(deadline.UTC().Truncate(time.Millisecond)) {
		t.Fatal("caller wait deadline not sent")
	}
	clientAwait(t, l.Done())
	clientClosed(t, l)
	if len(d.commands()) != 1 {
		t.Fatal("lease expiration unexpectedly contacted driver")
	}
	if err = l.Release(context.Background()); err != nil {
		t.Fatalf("expired release: %v", err)
	}
}

// clientExhaustRemaining waits for an observable clock tick so coarse clocks
// cannot return a result before the injected one-nanosecond lifetime has elapsed.
func clientExhaustRemaining(r Result) Result {
	r.Remaining = time.Nanosecond
	deadline := time.Now().Add(r.Remaining)
	for time.Now().Before(deadline) {
		runtime.Gosched()
	}
	return r
}

func TestSharedAcquireRejectsInvalidOrLateResults(t *testing.T) {
	tests := []struct {
		name   string
		change func(Result) Result
	}{
		{"missing lease", func(r Result) Result { r.LeaseID = ""; return r }},
		{"wrong weight", func(r Result) Result { r.Weight++; return r }},
		{"zero duration", func(r Result) Result { r.Remaining = 0; return r }},
		{"too long", func(r Result) Result { r.Remaining = time.Hour; return r }},
		{"terminal", func(r Result) Result { r.Status = Released; return r }},
		{"roundtrip exhausted", clientExhaustRemaining},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &clientStateDriver{after: func(_ context.Context, c Command, r Result, e error) (Result, error) {
				if c.Kind == Submit {
					r = tt.change(r)
				}
				return r, e
			}}
			c := clientNew(t, d, clientOptions())
			if l, err := c.Acquire(context.Background()); l != nil || !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("lease %v error %v", l, err)
			}
			calls := d.commands()
			if calls[len(calls)-1].Kind != Cancel {
				t.Fatal("missing cleanup")
			}
		})
	}
}

func TestSharedAmbiguousSubmitAndCleanupErrors(t *testing.T) {
	lost := errors.New("lost submit response")
	cleanup := errors.New("lost cleanup response")
	d := &clientStateDriver{after: func(_ context.Context, c Command, r Result, e error) (Result, error) {
		if c.Kind == Submit {
			return r, lost
		}
		if c.Kind == Cancel {
			return r, cleanup
		}
		return r, e
	}}
	c := clientNew(t, d, clientOptions())
	l, err := c.Acquire(context.Background())
	if l != nil || !errors.Is(err, lost) || !errors.Is(err, cleanup) {
		t.Fatalf("lease %v error %v", l, err)
	}
	calls := d.commands()
	if len(calls) != 2 || calls[1].Kind != Cancel || calls[0].Request.ID != calls[1].Request.ID {
		t.Fatal("cleanup changed identity or retried admission")
	}
	d.mu.Lock()
	_, r, e := Transition(d.state, time.Now(), Command{Kind: Observe, Request: calls[0].Request})
	d.mu.Unlock()
	if e != nil || r.Status != Cancelled {
		t.Fatal("ambiguous committed grant not cancelled")
	}
}

func TestSharedAcquireLateSuccessFailsClosed(t *testing.T) {
	d := &clientStateDriver{after: func(ctx context.Context, c Command, r Result, e error) (Result, error) {
		if c.Kind == Submit {
			<-ctx.Done()
		}
		return r, e
	}}
	o := clientOptions()
	o.OperationTimeout = 10 * time.Millisecond
	c := clientNew(t, d, o)
	if l, err := c.Acquire(context.Background()); l != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late success: %v %v", l, err)
	}
}

func TestSharedAcquisitionCancellationDuringGrant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &clientStateDriver{after: func(_ context.Context, c Command, r Result, e error) (Result, error) {
		if c.Kind == Submit {
			cancel()
		}
		return r, e
	}}
	c := clientNew(t, d, clientOptions())
	if l, err := c.Acquire(ctx); l != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled grant: %v %v", l, err)
	}
}

func TestSharedConservativeRoundTripDeadline(t *testing.T) {
	var completed time.Time
	d := &clientStateDriver{after: func(_ context.Context, c Command, r Result, e error) (Result, error) {
		if c.Kind == Submit || c.Kind == Renew {
			time.Sleep(30 * time.Millisecond)
			completed = time.Now()
		}
		return r, e
	}}
	o := clientOptions()
	o.Config.LeaseTTL = 300 * time.Millisecond
	c := clientNew(t, d, o)
	l := clientAcquire(t, c)
	if remaining := time.Until(l.deadline); remaining > 275*time.Millisecond {
		t.Fatalf("network RTT not subtracted: %s", remaining)
	}
	if err := l.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if l.deadline.After(completed.Add(275 * time.Millisecond)) {
		t.Fatal("renewal RTT not subtracted")
	}
}

func TestSharedRenewErrorsPermanentlyInvalidate(t *testing.T) {
	lost := errors.New("renew response lost")
	tests := []struct {
		name   string
		change func(context.Context, Result) (Result, error)
	}{
		{"transport", func(_ context.Context, r Result) (Result, error) { return r, lost }},
		{"wrong status", func(_ context.Context, r Result) (Result, error) { r.Status = Released; return r, nil }},
		{"wrong identity", func(_ context.Context, r Result) (Result, error) { r.LeaseID = "other"; return r, nil }},
		{"wrong weight", func(_ context.Context, r Result) (Result, error) { r.Weight++; return r, nil }},
		{"zero ttl", func(_ context.Context, r Result) (Result, error) { r.Remaining = 0; return r, nil }},
		{"excess ttl", func(_ context.Context, r Result) (Result, error) { r.Remaining = time.Hour; return r, nil }},
		{"elapsed ttl", func(_ context.Context, r Result) (Result, error) { return clientExhaustRemaining(r), nil }},
		{"timeout", func(ctx context.Context, r Result) (Result, error) { <-ctx.Done(); return r, nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var renews atomic.Int32
			d := &clientStateDriver{after: func(ctx context.Context, c Command, r Result, e error) (Result, error) {
				if c.Kind == Renew {
					renews.Add(1)
					return tt.change(ctx, r)
				}
				return r, e
			}}
			o := clientOptions()
			o.OperationTimeout = 30 * time.Millisecond
			c := clientNew(t, d, o)
			l := clientAcquire(t, c)
			if err := l.Renew(context.Background()); err == nil {
				t.Fatal("renew authorized invalid response")
			}
			clientClosed(t, l)
			if err := l.Renew(context.Background()); !errors.Is(err, ErrLeaseLost) {
				t.Fatal(err)
			}
			if renews.Load() != 1 {
				t.Fatal("invalid lease attempted resurrection")
			}
		})
	}
}

func TestSharedRenewCannotResurrectExpiredLease(t *testing.T) {
	entered := make(chan struct{})
	respond := make(chan struct{})
	d := &clientStateDriver{after: func(_ context.Context, c Command, r Result, e error) (Result, error) {
		if c.Kind == Renew {
			close(entered)
			<-respond
		}
		return r, e
	}}
	o := clientOptions()
	o.Config.LeaseTTL = 60 * time.Millisecond
	c := clientNew(t, d, o)
	l := clientAcquire(t, c)
	finished := make(chan struct{})
	var err error
	go func() { err = l.Renew(context.Background()); close(finished) }()
	clientAwait(t, entered)
	clientAwait(t, l.Done())
	close(respond)
	clientAwait(t, finished)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	clientClosed(t, l)
}

func TestSharedReleaseInvalidatesBeforeIOAndRetries(t *testing.T) {
	entered := make(chan struct{})
	respond := make(chan struct{})
	lost := errors.New("release response lost")
	var attempts atomic.Int32
	d := &clientStateDriver{after: func(_ context.Context, c Command, r Result, e error) (Result, error) {
		if c.Kind == Release && attempts.Add(1) == 1 {
			close(entered)
			<-respond
			return r, lost
		}
		return r, e
	}}
	c := clientNew(t, d, clientOptions())
	l := clientAcquire(t, c)
	finished := make(chan struct{})
	var err error
	go func() { err = l.Release(context.Background()); close(finished) }()
	clientAwait(t, entered)
	clientClosed(t, l)
	close(respond)
	clientAwait(t, finished)
	if !errors.Is(err, lost) {
		t.Fatal(err)
	}
	if err = l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	clientClosed(t, l)
}

func TestSharedReleaseRejectsWrongStatus(t *testing.T) {
	d := &clientStateDriver{after: func(_ context.Context, c Command, r Result, e error) (Result, error) {
		if c.Kind == Release {
			r.Status = Granted
		}
		return r, e
	}}
	c := clientNew(t, d, clientOptions())
	l := clientAcquire(t, c)
	if err := l.Release(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	clientClosed(t, l)
}

func TestSharedCancelledRenewAndReleaseDoNotCallDriver(t *testing.T) {
	d := &clientStateDriver{}
	c := clientNew(t, d, clientOptions())
	l := clientAcquire(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Renew(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	clientClosed(t, l)
	if err := l.Release(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(d.commands()) != 1 {
		t.Fatal("cancelled operations contacted driver")
	}
	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSharedConcurrentLeaseOperations(t *testing.T) {
	d := &clientStateDriver{}
	c := clientNew(t, d, clientOptions())
	l := clientAcquire(t, c)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = l.Valid()
				_ = l.Weight()
				_ = l.Done()
				switch n % 3 {
				case 0:
					err := l.Renew(context.Background())
					if err != nil && !errors.Is(err, ErrLeaseLost) {
						t.Error(err)
					}
				case 1:
					if err := l.Release(context.Background()); err != nil {
						t.Error(err)
					}
				}
			}
		}(i)
	}
	wg.Wait()
	clientClosed(t, l)
}

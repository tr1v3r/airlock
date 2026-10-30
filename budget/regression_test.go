package budget

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

// callbackLease exposes exact operation boundaries without scheduler sleeps.
type callbackLease struct {
	chunkLease
	renew   func(context.Context) error
	release func(context.Context) error
	valid   func() bool
}

func (l *callbackLease) Valid() bool {
	if l.valid != nil {
		return l.valid()
	}
	return l.chunkLease.Valid()
}
func (l *callbackLease) Renew(ctx context.Context) error {
	if l.renew != nil {
		return l.renew(ctx)
	}
	return l.chunkLease.Renew(ctx)
}
func (l *callbackLease) Release(ctx context.Context) error {
	if l.release != nil {
		return l.release(ctx)
	}
	return l.chunkLease.Release(ctx)
}

// waitPending waits only for a concrete synchronized queue state, not elapsed
// wall time. The deadline is a deadlock guard, not the interleaving mechanism.
func waitPending(t *testing.T, m *Manager, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		m.mu.Lock()
		got := len(m.pending)
		m.mu.Unlock()
		if got == n {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("pending=%d want%d", got, n)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestReturnFreezesBeforeBackendReissues(t *testing.T) {
	src := &fakeSource{capacity: 4}
	opts := baseOpts()
	opts.MaxHeld = 4
	opts.LowPeriods = 1
	a := mustManager(t, src, opts)
	bo := opts
	bo.MinHeld = 4
	b := mustManager(t, src, bo)
	chunk, err := src.Acquire(context.Background(), "a", 4)
	if err != nil {
		t.Fatal(err)
	}
	a.chunks = []chunkLease{&callbackLease{chunkLease: chunk, release: func(ctx context.Context) error {
		if err := chunk.Release(ctx); err != nil {
			return err
		}
		if p, e := a.TryAcquire("a", 4); e == nil {
			p.Release()
			t.Fatal("donated budget still admitted locally")
		}
		b.step()
		p, e := b.TryAcquire("b", 4)
		if e != nil {
			t.Fatal(e)
		}
		p.Release()
		return nil
	}}}
	_ = a.local.Resize(4)
	a.step()
	if a.Held() != 0 || b.Held() != 4 {
		t.Fatalf("A=%d B=%d", a.Held(), b.Held())
	}
}

func TestCloseFreezesDrainsAndRetriesAfterDeadline(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 4
	m := mustManager(t, src, opts)
	m.step()
	p, err := m.TryAcquire("a", 4)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close=%v", err)
	}
	if src.leases[0].released != 0 {
		t.Fatal("returned occupied chunk")
	}
	if _, err := m.TryAcquire("a", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("TryAcquire=%v", err)
	}
	if _, err := m.Acquire(context.Background(), "a", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Acquire=%v", err)
	}
	p.Release()
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.Held() != 0 || m.Stats().Capacity != 0 {
		t.Fatal("Close retained usable budget")
	}
}

func TestCloseCancelsRenewWithoutHoldingAdmissionMutex(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 4
	opts.OperationTimeout = time.Hour
	m := mustManager(t, src, opts)
	m.step()
	entered := make(chan struct{})
	finished := make(chan struct{})
	original := m.chunks[0]
	m.chunks[0] = &callbackLease{chunkLease: original, renew: func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }}
	go func() { m.step(); close(finished) }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("control RPC not canceled")
	}
	if _, err := m.TryAcquire("a", 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestExpiredChunkStopsAdmissionBetweenControlTicks(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 4
	m := mustManager(t, src, opts)
	m.step()
	src.leases[0].mu.Lock()
	src.leases[0].invalid = true
	src.leases[0].mu.Unlock()
	if p, err := m.TryAcquire("a", 1); err == nil {
		p.Release()
		t.Fatal("expired budget admitted")
	}
	if m.Held() != 0 || m.Stats().Capacity != 0 {
		t.Fatal("expired holding remains usable")
	}
}

func TestExpiredChunkKeepsOutstandingPermitsChargedLocally(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 6
	opts.Denominations = []int{4, 2}
	m := mustManager(t, src, opts)
	m.step()
	p, err := m.TryAcquire("a", 3)
	if err != nil {
		t.Fatal(err)
	}
	src.leases[0].mu.Lock()
	src.leases[0].invalid = true
	src.leases[0].mu.Unlock()
	if q, e := m.TryAcquire("b", 1); e == nil {
		q.Release()
		t.Fatal("new chunk hid expired outstanding occupancy")
	}
	if m.Held() != 2 || m.Stats().InUse != 3 {
		t.Fatal("forgot outstanding occupancy")
	}
	p.Release()
	q, err := m.TryAcquire("b", 2)
	if err != nil {
		t.Fatal(err)
	}
	q.Release()
}

func TestWeightedColdStartPreservesDemandUntilBudgetFits(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	m := mustManager(t, src, opts)
	type outcome struct {
		p *airlock.Permit
		e error
	}
	done := make(chan outcome, 1)
	go func() { p, e := m.Acquire(context.Background(), "heavy", 16); done <- outcome{p, e} }()
	waitPending(t, m, 1)
	m.step()
	select {
	case got := <-done:
		if got.e != nil {
			t.Fatal(got.e)
		}
		if m.Stats().InUse != 16 {
			t.Fatal("wrongweight")
		}
		got.p.Release()
	case <-time.After(time.Second):
		t.Fatal("weighted request did not progress")
	}
}

func TestWaitingWeightsRemainBoundedAndCancellationCleansDemand(t *testing.T) {
	src := &fakeSource{blocked: true}
	opts := baseOpts()
	opts.LocalQueue = 1
	m := mustManager(t, src, opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := m.Acquire(ctx, "heavy", 16); done <- e }()
	waitPending(t, m, 1)
	if _, e := m.Acquire(context.Background(), "other", 1); !errors.Is(e, airlock.ErrQueueFull) {
		t.Fatalf("overflow=%v", e)
	}
	if _, e := m.TryAcquire("other", 1); !errors.Is(e, airlock.ErrWouldBlock) {
		t.Fatalf("bypass=%v", e)
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	waitPending(t, m, 0)
	m.step()
	if m.Held() != 0 {
		t.Fatal("canceled demand acquired chunks")
	}
}

func TestReturnTriesSmallerChunksWhenLargestCannotFit(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.LowPeriods = 1
	m := mustManager(t, src, opts)
	for _, w := range []int{16, 4, 1} {
		c, _ := src.Acquire(context.Background(), "a", w)
		m.chunks = append(m.chunks, c)
	}
	_ = m.local.Resize(21)
	p, e := m.TryAcquire("a", 4)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Release()
	m.step()
	if m.Held() != 16 {
		t.Fatalf("Held=%d want16", m.Held())
	}
}

func TestInsufficientFreeCapacityFallsBackToSmallerChunk(t *testing.T) {
	src := &fakeSource{capacity: 4, live: 3}
	opts := baseOpts()
	opts.MinHeld = 2
	opts.Denominations = []int{2, 1}
	m := mustManager(t, src, opts)
	m.step()
	if m.Held() != 1 {
		t.Fatalf("Held=%d want1", m.Held())
	}
	p, e := m.TryAcquire("a", 1)
	if e != nil {
		t.Fatal(e)
	}
	p.Release()
}

func TestTryAndAcquireValidation(t *testing.T) {
	m := mustManager(t, &fakeSource{}, baseOpts())
	for _, tc := range []struct {
		tenant string
		weight int
		want   error
	}{{"", 1, airlock.ErrInvalidTenant}, {"a", 0, airlock.ErrInvalidWeight}, {"a", 65, airlock.ErrWeightExceedsCapacity}} {
		if _, e := m.TryAcquire(tc.tenant, tc.weight); !errors.Is(e, tc.want) {
			t.Fatal(e)
		}
		if _, e := m.Acquire(context.Background(), tc.tenant, tc.weight); !errors.Is(e, tc.want) {
			t.Fatal(e)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := m.Acquire(ctx, "a", 1); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	for _, fn := range []func(){func() { _, _ = m.Acquire(nil, "a", 1) }, func() { _ = m.Close(nil) }} { //nolint:staticcheck // SA1012: verify the documented nil-context panic contracts.
		func() {
			defer func() {
				if recover() == nil {
					t.Error("nil context did not panic")
				}
			}()
			fn()
		}()
	}
}

func TestConcurrentCloseDoesNotReleaseActivePermit(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 1
	m := mustManager(t, src, opts)
	m.step()
	p, _ := m.TryAcquire("a", 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = m.Close(ctx) }()
	}
	wg.Wait()
	if src.leases[0].released != 0 {
		t.Fatal("closed before drain")
	}
	p.Release()
	if e := m.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
}

func TestRealSharedFallbackUsesRemainingPoolCapacity(t *testing.T) {
	pool := &poolHarness{}
	opts := integrationOptions(pool, "a")
	source, err := newSharedSource(opts)
	if err != nil {
		t.Fatal(err)
	}
	occupied, err := source.Acquire(context.Background(), "a", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Release(context.Background()) }()
	opts.GatewayID = "b"
	opts.MinHeld = 2
	bs, err := newSharedSource(opts)
	if err != nil {
		t.Fatal(err)
	}
	b := mustManager(t, bs, opts)
	b.step()
	defer func() { _ = b.Close(context.Background()) }()
	if b.Held() != 1 {
		t.Fatalf("Held=%d want1 despite larger denomination wait rejection", b.Held())
	}
}

func TestResizeZeroPreservesExistingChunkLease(t *testing.T) {
	pool := &poolHarness{}
	opts := integrationOptions(pool, "a")
	opts.MinHeld = 4
	source, err := newSharedSource(opts)
	if err != nil {
		t.Fatal(err)
	}
	a := mustManager(t, source, opts)
	a.step()
	defer func() { _ = a.Close(context.Background()) }()
	_, err = pool.Resize(context.Background(), airlock.ResizeRequest{Resource: testResource, Capacity: 0, ExpectedVersion: pool.capacityVersion()})
	if err != nil {
		t.Fatal(err)
	}
	a.step()
	p, err := a.TryAcquire("existing", 4)
	if err != nil {
		t.Fatalf("Shared Resize0 does not revoke existing lease: %v", err)
	}
	p.Release()
}

func TestSharedLeaseInvalidationClosesLocalAdmission(t *testing.T) {
	opts := integrationOptions(&poolHarness{}, "a")
	source, e := newSharedSource(opts)
	if e != nil {
		t.Fatal(e)
	}
	c, e := source.Acquire(context.Background(), "a", 4)
	if e != nil {
		t.Fatal(e)
	}
	m := mustManager(t, source, opts)
	m.chunks = []chunkLease{c}
	_ = m.local.Resize(4)
	if e := c.Release(context.Background()); e != nil {
		t.Fatal(e)
	}
	if p, e := m.TryAcquire("a", 1); e == nil {
		p.Release()
		t.Fatal("Shared invalidation not applied")
	}
	if m.Held() != 0 {
		t.Fatal("invalid shared chunk counted")
	}
}

func TestTryAcquireRechecksValidityAfterLocalGrant(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 4
	m := mustManager(t, src, opts)
	m.step()
	var calls int
	m.chunks[0] = &callbackLease{chunkLease: m.chunks[0], valid: func() bool { calls++; return calls < 3 }}
	if p, e := m.TryAcquire("a", 1); e == nil {
		p.Release()
		t.Fatal("permit escaped final validity check")
	}
	if m.Stats().InUse != 0 {
		t.Fatal("rejected local permit leaked")
	}
}

func TestQueuedAcquireRechecksExpiredGrant(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 4
	m := mustManager(t, src, opts)
	m.step()
	holder, e := m.TryAcquire("holder", 4)
	if e != nil {
		t.Fatal(e)
	}
	var invalid atomic.Bool
	var once sync.Once
	checked := make(chan struct{})
	m.chunks[0] = &callbackLease{chunkLease: m.chunks[0], valid: func() bool {
		if invalid.Load() {
			if m.local.Stats().InUse == 1 {
				once.Do(func() { close(checked) })
			}
			return false
		}
		return true
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		p, e := m.Acquire(ctx, "queued", 1)
		if p != nil {
			p.Release()
		}
		done <- e
	}()
	waitPending(t, m, 1)
	// Synchronize actual core enqueue before releasing the holder.
	end := time.Now().Add(time.Second)
	for m.local.Stats().Queued != 1 {
		if time.Now().After(end) {
			t.Fatal("waiter did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	invalid.Store(true)
	holder.Release()
	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("no post-grant validity check")
	}
	if m.Stats().InUse != 0 {
		t.Fatal("undelivered expired permit not released")
	}
	select {
	case e := <-done:
		t.Fatalf("expired waiter unexpectedly completed: %v", e)
	default:
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestCancelHeavyWaiterReopensLightQueueWithoutControlTick(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 2
	m := mustManager(t, src, opts)
	m.step()
	heavyCtx, cancelHeavy := context.WithCancel(context.Background())
	heavy := make(chan error, 1)
	go func() { _, e := m.Acquire(heavyCtx, "heavy", 4); heavy <- e }()
	waitPending(t, m, 1)
	lightCtx, cancelLight := context.WithTimeout(context.Background(), time.Second)
	defer cancelLight()
	light := make(chan *airlock.Permit, 1)
	go func() {
		p, e := m.Acquire(lightCtx, "light", 1)
		if e == nil {
			light <- p
		}
	}()
	waitPending(t, m, 2)
	cancelHeavy()
	if e := <-heavy; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	select {
	case p := <-light:
		p.Release()
	case <-lightCtx.Done():
		t.Fatal("heavy cancellation did not reopen local queue")
	}
}

func TestCloseCancelsQueuedCallerAndRetainsFailedCleanup(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 1
	m := mustManager(t, src, opts)
	m.step()
	holder, _ := m.TryAcquire("holder", 1)
	done := make(chan error, 1)
	go func() { _, e := m.Acquire(context.Background(), "queued", 1); done <- e }()
	waitPending(t, m, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = m.Close(ctx)
	if e := <-done; !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	waitPending(t, m, 0)
	holder.Release()
	src.leases[0].releaseErr = errors.New("reply lost")
	if e := m.Close(context.Background()); e == nil {
		t.Fatal("cleanup error hidden")
	}
	if len(m.chunks) != 1 {
		t.Fatal("unknown cleanup identity forgotten")
	}
	src.leases[0].releaseErr = nil
	if e := m.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	if len(m.chunks) != 0 {
		t.Fatal("confirmed cleanup retained")
	}
}

func TestRejectNonfiniteDemandOptions(t *testing.T) {
	for _, tune := range []func(*Options){func(o *Options) { o.Headroom = math.NaN() }, func(o *Options) { o.Headroom = 0.5 }, func(o *Options) { o.Headroom = math.Inf(1) }, func(o *Options) { o.LowUtilization = math.NaN() }, func(o *Options) { o.LowUtilization = math.Inf(1) }} {
		opts := baseOpts()
		tune(&opts)
		if _, e := newManager(&fakeSource{}, opts); e == nil {
			t.Fatal("nonfinite tuning accepted")
		}
	}
}

func TestCloseDeadlineDoesNotWaitForUncooperativeRenew(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 4
	m := mustManager(t, src, opts)
	m.step()
	entered, allow := make(chan struct{}), make(chan struct{})
	finished := make(chan struct{})
	m.chunks[0] = &callbackLease{chunkLease: m.chunks[0], renew: func(context.Context) error { close(entered); <-allow; return nil }}
	go func() { m.step(); close(finished) }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Close(ctx) }()
	end := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		stopped := m.stopped
		m.mu.Unlock()
		if stopped {
			break
		}
		if time.Now().After(end) {
			t.Fatal("Close did not freeze")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("Close waited for backend")
	}
	close(allow)
	<-finished
	if e := m.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	m.step()
}

func TestCloseContextCancelsWhileWaitingForPermitDrain(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 1
	m := mustManager(t, src, opts)
	m.step()
	p, _ := m.TryAcquire("a", 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Close(ctx) }()
	end := time.Now().Add(time.Second)
	for len(m.control) != 0 {
		if time.Now().After(end) {
			t.Fatal("Close did not take control")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if src.leases[0].released != 0 {
		t.Fatal("released before drain")
	}
	p.Release()
	if e := m.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
}

func TestAcquireFastPathRechecksValidity(t *testing.T) {
	src := &fakeSource{}
	opts := baseOpts()
	opts.MinHeld = 4
	m := mustManager(t, src, opts)
	m.step()
	var calls int
	m.chunks[0] = &callbackLease{chunkLease: m.chunks[0], valid: func() bool { calls++; return calls < 2 }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		p, e := m.Acquire(ctx, "a", 1)
		if p != nil {
			p.Release()
		}
		done <- e
	}()
	waitPending(t, m, 1)
	if m.Stats().InUse != 0 {
		t.Fatal("invalid fastpath permit leaked")
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestNewReportsManagerValidationAfterSharedConstruction(t *testing.T) {
	opts := integrationOptions(&poolHarness{}, "a")
	opts.MaxHeld = -1
	if _, e := New(opts); e == nil {
		t.Fatal("invalid manager options accepted")
	}
}

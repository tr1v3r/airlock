package airlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func sharedConcurrentOptions(clients int) SharedOptions {
	o := clientOptions()
	o.Config.Admission = Config{Capacity: 7, MaxQueued: clients, MaxQueuedPerTenant: clients}
	// This fixture serializes whole-snapshot transitions, including observations.
	// Larger loads grow both the number of grant waves and snapshot work per
	// operation. Budget those separately instead of treating the race-instrumented
	// fake as a fixed-throughput backend. The normal <=32-client load is unchanged.
	const baseClients, maxWeight = 32, 3
	perWave := o.Config.Admission.Capacity / maxWeight
	waves := (clients + perWave - 1) / perWave
	baseWaves := (baseClients + perWave - 1) / perWave
	waveScale := (waves + baseWaves - 1) / baseWaves
	snapshotScale := (clients + baseClients - 1) / baseClients
	budget := 30 * time.Second * time.Duration(waveScale*snapshotScale)
	o.Config.LeaseTTL = budget
	o.Config.MaxWait = budget
	o.Config.SubmitWindow = budget
	// Keep terminal records until the final whole-workload assertion. Protocol GC
	// and no-resurrection behavior are tested separately with short retention.
	o.Config.Retention = 2 * budget
	o.Config.MaxRecords = 2 * clients
	o.OperationTimeout = 5 * time.Second * time.Duration(snapshotScale)
	// Keep the same contention model in both modes: the longer workload budget,
	// not a different scheduler/driver or looser assertion, must enable progress.
	o.PollInterval = 5 * time.Millisecond
	return o
}

func TestSharedConcurrentWorkloadBudget(t *testing.T) {
	for _, tc := range []struct {
		clients                  int
		budget, operationTimeout time.Duration
	}{
		{16, 30 * time.Second, 5 * time.Second},
		{24, 30 * time.Second, 5 * time.Second},
		{32, 30 * time.Second, 5 * time.Second},
		{96, 270 * time.Second, 15 * time.Second},
		{128, 480 * time.Second, 20 * time.Second},
	} {
		t.Run(fmt.Sprint(tc.clients), func(t *testing.T) {
			o := sharedConcurrentOptions(tc.clients)
			if o.Config.LeaseTTL != tc.budget || o.Config.MaxWait != tc.budget || o.Config.SubmitWindow != tc.budget || o.Config.Retention != 2*tc.budget || o.OperationTimeout != tc.operationTimeout || o.PollInterval != 5*time.Millisecond {
				t.Fatalf("unexpected workload budget: %+v", o)
			}
			if _, err := NewShared(&clientStateDriver{}, o); err != nil {
				t.Fatalf("workload budget exceeds protocol bounds: %v", err)
			}
		})
	}
}

func sharedAssertDrained(t *testing.T, d *clientStateDriver, want int) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	var s durableSnapshot
	if err := json.Unmarshal(d.state, &s); err != nil {
		t.Fatal(err)
	}
	if len(s.Records) != want {
		t.Fatalf("retained %d requests, want %d", len(s.Records), want)
	}
	for id, rec := range s.Records {
		if rec.Status != Released && rec.Status != Cancelled {
			t.Errorf("request %s did not converge to released/cancelled: %s", id, rec.Status)
		}
	}
	if s.Protected != "" || len(s.Queues) != 0 {
		t.Fatal("terminal workload left a protected or queued request")
	}
}

func TestSharedConcurrentWeightedLifecycle(t *testing.T) {
	clients, rounds := 32, 2
	if os.Getenv("AIRLOCK_STRESS") == "1" {
		clients, rounds = 128, 8
	}
	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			d := &clientStateDriver{}
			o := sharedConcurrentOptions(clients)
			c := clientNew(t, d, o)
			ctx, cancel := context.WithTimeout(context.Background(), o.Config.MaxWait)
			defer cancel()
			start := make(chan struct{})
			var workers sync.WaitGroup
			var used, completed atomic.Int64
			started := time.Now()
			defer func() {
				counts := make(map[Operation]int)
				for _, cmd := range d.commands() {
					counts[cmd.Kind]++
				}
				t.Logf("clients=%d budget=%s elapsed=%s completed=%d used=%d submit=%d observe=%d renew=%d release=%d cancel=%d",
					clients, o.Config.MaxWait, time.Since(started), completed.Load(), used.Load(),
					counts[Submit], counts[Observe], counts[Renew], counts[Release], counts[Cancel])
			}()
			for i := 0; i < clients; i++ {
				workers.Add(1)
				go func(i int) {
					defer workers.Done()
					<-start
					weight := 1 + i%3
					lease, err := c.AcquireNFor(ctx, fmt.Sprintf("tenant-%d", i%4), weight)
					if err != nil {
						t.Errorf("acquire %d: %v", i, err)
						return
					}
					if total := used.Add(int64(weight)); total > int64(o.Config.Admission.Capacity) {
						t.Errorf("live work owns %d > capacity %d", total, o.Config.Admission.Capacity)
					}
					if lease.Weight() != weight || !lease.Valid() {
						t.Errorf("invalid allocation for worker %d", i)
					}
					for j := 0; j < 2; j++ {
						if err := lease.Renew(ctx); err != nil {
							t.Errorf("renew worker %d: %v", i, err)
						}
					}
					// Finish all simulated work before release can grant another owner.
					used.Add(-int64(weight))
					for j := 0; j < 2; j++ {
						if err := lease.Release(ctx); err != nil {
							t.Errorf("release worker %d: %v", i, err)
						}
					}
					if lease.Valid() {
						t.Errorf("worker %d lease remained valid after release", i)
					}
					select {
					case <-lease.Done():
					default:
						t.Errorf("worker %d Done remained open", i)
					}
					completed.Add(1)
				}(i)
			}
			close(start)
			workers.Wait()
			if completed.Load() != int64(clients) || used.Load() != 0 {
				t.Fatalf("completed=%d want=%d used=%d", completed.Load(), clients, used.Load())
			}
			sharedAssertDrained(t, d, clients)
		})
	}
}

func TestSharedConcurrentQueuedCancellation(t *testing.T) {
	clients := 24
	if os.Getenv("AIRLOCK_STRESS") == "1" {
		clients = 96
	}
	queued := make(chan struct{}, clients)
	d := &clientStateDriver{after: func(_ context.Context, cmd Command, r Result, err error) (Result, error) {
		if cmd.Kind == Submit && r.Status == Queued {
			queued <- struct{}{}
		}
		return r, err
	}}
	o := sharedConcurrentOptions(clients)
	c := clientNew(t, d, o)
	ctx, cancel := context.WithTimeout(context.Background(), o.Config.MaxWait)
	defer cancel()
	holder, err := c.AcquireN(ctx, o.Config.Admission.Capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Release(context.Background()) }()
	cancels := make([]context.CancelFunc, clients)
	results := make([]chan error, clients)
	var workers sync.WaitGroup
	for i := range cancels {
		waitCtx, stop := context.WithCancel(ctx)
		cancels[i] = stop
		defer stop()
		results[i] = make(chan error, 1)
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			lease, err := c.AcquireFor(waitCtx, fmt.Sprintf("waiter-%d", i))
			if err == nil {
				err = lease.Release(ctx)
			}
			results[i] <- err
		}(i)
	}
	defer func() { cancel(); workers.Wait() }()
	for i := 0; i < clients; i++ {
		select {
		case <-queued:
		case <-ctx.Done():
			t.Fatal("not all requests entered the queue")
		}
	}
	// Keep capacity occupied until every cancelled acquisition has finished cleanup.
	for i := 0; i < clients; i += 2 {
		cancels[i]()
	}
	for i := 0; i < clients; i += 2 {
		select {
		case err := <-results[i]:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("cancelled waiter %d: %v", i, err)
			}
		case <-ctx.Done():
			t.Fatal("cancelled acquisitions did not join")
		}
	}
	if err := holder.Release(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < clients; i += 2 {
		select {
		case err := <-results[i]:
			if err != nil {
				t.Errorf("surviving waiter %d: %v", i, err)
			}
		case <-ctx.Done():
			t.Fatal("surviving acquisitions did not finish")
		}
	}
	workers.Wait()
	sharedAssertDrained(t, d, clients+1)
}

func TestSharedConcurrentRenewReleaseUnknownOutcome(t *testing.T) {
	entered := make(chan struct{})
	respond := make(chan struct{})
	lostRenew := errors.New("renew response lost after commit")
	lostRelease := errors.New("release response lost after commit")
	var releaseCalls atomic.Int32
	d := &clientStateDriver{after: func(ctx context.Context, cmd Command, r Result, err error) (Result, error) {
		switch cmd.Kind {
		case Renew:
			close(entered)
			select {
			case <-respond:
				return r, lostRenew
			case <-ctx.Done():
				return r, ctx.Err()
			}
		case Release:
			if releaseCalls.Add(1) == 1 {
				return r, lostRelease
			}
		}
		return r, err
	}}
	o := sharedConcurrentOptions(16)
	c := clientNew(t, d, o)
	lease := clientAcquire(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var workers sync.WaitGroup
	var respondOnce sync.Once
	unblock := func() { respondOnce.Do(func() { close(respond) }) }
	// Register cleanup before the first goroutine: even clientAwait/Fatal must
	// cancel blocked I/O, unblock the response and join every operation.
	defer func() { cancel(); unblock(); workers.Wait() }()
	renewResult := make(chan error, 1)
	workers.Add(1)
	go func() { defer workers.Done(); renewResult <- lease.Renew(ctx) }()
	clientAwait(t, entered)
	const releasers = 16
	results := make(chan error, releasers)
	for i := 0; i < releasers; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); results <- lease.Release(ctx) }()
	}
	unblock()
	if err := <-renewResult; !errors.Is(err, lostRenew) {
		t.Fatalf("renew error lost: %v", err)
	}
	workers.Wait()
	close(results)
	unknown := 0
	for err := range results {
		switch {
		case errors.Is(err, lostRelease):
			unknown++
		case err != nil:
			t.Errorf("unexpected release result: %v", err)
		}
	}
	if unknown != 1 || releaseCalls.Load() != releasers {
		t.Fatalf("unknown=%d release attempts=%d", unknown, releaseCalls.Load())
	}
	clientClosed(t, lease)
	if err := lease.Renew(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("uncertain renewal resurrected lease: %v", err)
	}
	sharedAssertDrained(t, d, 1)
	for _, cmd := range d.commands() {
		if cmd.Request.ID != lease.request.ID || ((cmd.Kind == Renew || cmd.Kind == Release) && cmd.LeaseID != lease.id) {
			t.Fatal("concurrent recovery changed request or lease identity")
		}
	}
}

func TestSharedConcurrentExpiryRejectsLateRenewal(t *testing.T) {
	entered, respond := make(chan struct{}), make(chan struct{})
	d := &clientStateDriver{after: func(ctx context.Context, cmd Command, r Result, err error) (Result, error) {
		if cmd.Kind == Renew {
			close(entered)
			select {
			case <-respond:
			case <-ctx.Done():
				return r, ctx.Err()
			}
		}
		return r, err
	}}
	o := sharedConcurrentOptions(32)
	o.Config.Admission.Capacity = 1
	c := clientNew(t, d, o)
	lease := clientAcquire(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var operations, readers sync.WaitGroup
	var respondOnce sync.Once
	unblock := func() { respondOnce.Do(func() { close(respond) }) }
	defer func() { cancel(); unblock(); readers.Wait(); operations.Wait() }()
	renewResult := make(chan error, 1)
	operations.Add(1)
	go func() { defer operations.Done(); renewResult <- lease.Renew(ctx) }()
	clientAwait(t, entered)
	// Model a process resuming after its conservative local deadline. Do not wait
	// for a real sleep/timer ordering; the backend renewal has already committed.
	lease.mu.Lock()
	lease.deadline = time.Now().Add(-time.Millisecond)
	lease.mu.Unlock()
	for i := 0; i < 32; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			if lease.Valid() {
				t.Error("expired local lease authorized work while renewal response was blocked")
			}
			select {
			case <-lease.Done():
			default:
				t.Error("Valid failed to close Done on expiry")
			}
		}()
	}
	readers.Wait()
	released := make(chan error, 1)
	operations.Add(1)
	go func() { defer operations.Done(); released <- lease.Release(ctx) }()
	unblock()
	if err := <-renewResult; !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late successful response revived expired lease: %v", err)
	}
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	clientClosed(t, lease)
	if err := lease.Renew(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("closed lease retried renewal: %v", err)
	}
	sharedAssertDrained(t, d, 1)
	next, err := c.Acquire(ctx)
	if err != nil {
		t.Fatalf("expired/released allocation leaked capacity: %v", err)
	}
	if err := next.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSharedAcquireUnknownCleanupRemainsBounded(t *testing.T) {
	lostSubmit := errors.New("submit response lost")
	lostCleanup := errors.New("cancel response lost")
	d := &clientStateDriver{after: func(_ context.Context, cmd Command, r Result, err error) (Result, error) {
		if cmd.Kind == Submit {
			return r, lostSubmit
		}
		if cmd.Kind == Cancel {
			return r, lostCleanup
		}
		return r, err
	}}
	c := clientNew(t, d, sharedConcurrentOptions(8))
	lease, err := c.Acquire(context.Background())
	if lease != nil || !errors.Is(err, lostSubmit) || !errors.Is(err, lostCleanup) {
		t.Fatalf("unknown acquisition authorized work or lost errors: %v %v", lease, err)
	}
	calls := d.commands()
	if len(calls) != 2 || calls[0].Kind != Submit || calls[1].Kind != Cancel {
		t.Fatal("ambiguous acquisition automatically retried admission or cleanup")
	}
	sharedAssertDrained(t, d, 1)
	result, err := d.Submit(context.Background(), calls[0].Request)
	if !errors.Is(err, lostSubmit) || result.Status != Cancelled {
		t.Fatalf("retry of original identity revived ambiguous grant: %+v %v", result, err)
	}
}

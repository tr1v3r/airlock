package airlock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func localWaitQueued(ctx context.Context, t *testing.T, a *Airlock, want int) {
	t.Helper()
	for {
		if got := a.Stats().Queued; got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("queue barrier: want=%d got=%+v: %v", want, a.Stats(), ctx.Err())
		default:
			runtime.Gosched()
		}
	}
}

func localCheckConcurrentAccounting(t *testing.T, a *Airlock, active *atomic.Int64, weight int) {
	t.Helper()
	if held := active.Add(int64(weight)); held > int64(a.cfg.Capacity) {
		t.Errorf("business tokens=%d exceed capacity=%d", held, a.cfg.Capacity)
	}
	s := a.Stats() // A mutex-protected, internally consistent implementation view.
	if s.InUse < weight || s.InUse > s.Capacity || s.ActivePermits < 1 || s.ActivePermits > s.InUse || s.Queued < 0 || s.WaitingTenants > s.Queued {
		t.Errorf("invalid concurrent snapshot: %+v weight=%d", s, weight)
	}
	runtime.Gosched()
	// Leave the business section BEFORE returning the permit to the scheduler.
	active.Add(-int64(weight))
}

func TestLocalConcurrentStorm(t *testing.T) {
	workers, waves := 256, 3
	if os.Getenv("AIRLOCK_STRESS") == "1" {
		workers, waves = 2048, 12
	}
	for _, priority := range []bool{false, true} {
		t.Run(fmt.Sprintf("priority=%v/workers=%d", priority, workers), func(t *testing.T) {
			cfg := Config{Capacity: 31, MaxQueued: workers * 3, MaxQueuedPerTenant: workers}
			if priority {
				cfg.Priority = &PriorityPolicy{
					Groups:       []PriorityGroup{{Name: "high", Turns: 8, MaxQueued: workers}, {Name: "medium", Turns: 3, MaxQueued: workers}, {Name: "low", Turns: 1, MaxQueued: workers}},
					DefaultGroup: "low", TenantGroups: make(map[string]string),
				}
				for i := range 24 {
					cfg.Priority.TenantGroups[fmt.Sprint(i)] = cfg.Priority.Groups[i%3].Name
				}
			}
			a, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for wave := 0; wave < waves; wave++ {
				t.Run(fmt.Sprintf("wave=%d", wave), func(t *testing.T) {
					localConcurrentWave(t, a, workers)
				})
			}
		})
	}
}

func localConcurrentWave(t *testing.T, a *Airlock, workers int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	var wg sync.WaitGroup
	var active atomic.Int64
	first, err := a.AcquireN(ctx, 16)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	second, err := a.TryAcquireN(15)
	if err != nil {
		cancel()
		first.Release()
		t.Fatal(err)
	}
	start := make(chan struct{})
	raceStart := make(chan struct{})
	var startOnce, raceOnce sync.Once
	openStart := func() { startOnce.Do(func() { close(start) }) }
	openRace := func() { raceOnce.Do(func() { close(raceStart) }) }
	// Registered before workers launch: a failed barrier must still unblock,
	// cancel, release and join every child instead of poisoning later waves.
	defer func() {
		cancel()
		openStart()
		openRace()
		first.Release()
		second.Release()
		wg.Wait()
	}()
	type outcome struct {
		id      int
		granted bool
		err     error
	}
	outcomes := make(chan outcome, workers)
	cancels := make([]context.CancelFunc, workers)
	for i := range workers {
		requestCtx, requestCancel := context.WithCancel(ctx)
		cancels[i] = requestCancel
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer requestCancel()
			<-start
			weight := i%a.cfg.Capacity + 1
			tenant := fmt.Sprint(i % 24)
			var p *Permit
			var acquireErr error
			if i%2 == 0 {
				p, acquireErr = a.TryAcquireNFor(tenant, weight)
				if errors.Is(acquireErr, ErrWouldBlock) {
					if p != nil {
						t.Error("failed Try returned a permit")
						p.Release()
					}
					p, acquireErr = a.AcquireNFor(requestCtx, tenant, weight)
				}
			} else {
				p, acquireErr = a.AcquireNFor(requestCtx, tenant, weight)
			}
			if acquireErr == nil && p != nil {
				localCheckConcurrentAccounting(t, a, &active, weight)
				var releases sync.WaitGroup
				for range 3 {
					releases.Add(1)
					go func() { defer releases.Done(); p.Release() }()
				}
				releases.Wait()
			} else if p != nil {
				t.Error("failed acquisition retained a permit")
				p.Release()
			}
			outcomes <- outcome{i, p != nil, acquireErr}
		}()
	}
	openStart()
	localWaitQueued(ctx, t, a, workers)
	// All arrivals are now pending; cancellation and releasing both holders race
	// at one barrier. No sleep or goroutine launch ordering determines fairness.
	for i, stop := range cancels {
		if i%3 == 0 {
			wg.Add(1)
			go func() { defer wg.Done(); <-raceStart; stop() }()
		}
	}
	for _, held := range []*Permit{first, second} {
		wg.Add(1)
		go func() { defer wg.Done(); <-raceStart; held.Release() }()
	}
	// Try callers contend with queued admissions, release and cancellation.
	// Their success/failure both preserve accounting; the model checks ordering.
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-raceStart
			for range 8 {
				weight := i%7 + 1
				p, tryErr := a.TryAcquireNFor(fmt.Sprint(i%24), weight)
				if tryErr == nil && p != nil {
					localCheckConcurrentAccounting(t, a, &active, weight)
					p.Release()
				} else if !errors.Is(tryErr, ErrWouldBlock) || p != nil {
					t.Errorf("unexpected Try result: permit=%v err=%v", p != nil, tryErr)
					p.Release()
				}
				runtime.Gosched()
			}
		}()
	}
	openRace()
	seen := make([]bool, workers)
	granted, cancelled := 0, 0
	for range workers {
		select {
		case result := <-outcomes:
			if seen[result.id] {
				t.Errorf("request=%d reached a terminal state twice", result.id)
			}
			seen[result.id] = true
			switch {
			case result.err == nil && result.granted:
				granted++
			case errors.Is(result.err, context.Canceled) && !result.granted && result.id%3 == 0:
				cancelled++
			default:
				t.Errorf("request=%d unexpected terminal result: granted=%v err=%v", result.id, result.granted, result.err)
			}
		case <-ctx.Done():
			t.Fatalf("storm did not finish: granted=%d cancelled=%d stats=%+v: %v", granted, cancelled, a.Stats(), ctx.Err())
		}
	}
	wg.Wait()
	for id, done := range seen {
		if !done {
			t.Errorf("request=%d has no terminal result", id)
		}
	}
	if got := active.Load(); got != 0 {
		t.Errorf("business token leak: %d", got)
	}
	if got := a.Stats(); got != (Stats{Capacity: a.cfg.Capacity}) {
		t.Fatalf("post-storm queue/permit leak: %+v", got)
	}
	// Reuse every token after each wave, rather than only constructing a fresh
	// limiter; catches accounting that would strand subsequent requests.
	p, err := a.TryAcquireN(a.cfg.Capacity)
	if err != nil || p == nil {
		t.Fatalf("cannot reuse full capacity after storm: permit=%v err=%v", p != nil, err)
	}
	p.Release()
}

func TestLocalMaxIntConcurrentRelease(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	a, err := New(Config{Capacity: maxInt, MaxQueued: 2, MaxQueuedPerTenant: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var holders []*Permit
	for _, weight := range []int{maxInt - 2, 1, 1} {
		p, acquireErr := a.TryAcquireN(weight)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		holders = append(holders, p)
		defer p.Release()
	}
	if p, tryErr := a.TryAcquireN(2); p != nil || !errors.Is(tryErr, ErrWouldBlock) {
		p.Release()
		t.Fatalf("MaxInt over-admission: permit=%v err=%v", p != nil, tryErr)
	}
	result := make(chan localCallResult, 1)
	received := false
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p, acquireErr := a.AcquireNFor(ctx, "whole", maxInt)
		result <- localCallResult{p, acquireErr}
	}()
	defer func() {
		cancel()
		for _, p := range holders {
			p.Release()
		}
		wg.Wait()
		if !received {
			got := <-result
			got.permit.Release()
		}
	}()
	localWaitQueued(ctx, t, a, 1)
	start := make(chan struct{})
	for _, p := range holders {
		for range 16 {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; p.Release() }()
		}
	}
	close(start)
	var whole *Permit
	select {
	case got := <-result:
		received = true
		whole = got.permit
		defer whole.Release()
		if got.err != nil || whole == nil {
			t.Fatalf("whole-capacity waiter: permit=%v err=%v", whole != nil, got.err)
		}
	case <-ctx.Done():
		t.Fatal("MaxInt waiter did not complete")
	}
	wg.Wait()
	if got := a.Stats(); got != (Stats{Capacity: maxInt, InUse: maxInt, ActivePermits: 1}) {
		t.Fatal(got)
	}
	whole.Release()
	if got := a.Stats(); got != (Stats{Capacity: maxInt}) {
		t.Fatal(got)
	}
}

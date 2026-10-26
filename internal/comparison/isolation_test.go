package comparison

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

const (
	isolationTotal = 12
	isolationHigh  = 4
	isolationLow   = isolationTotal - isolationHigh
)

// This adapter composes public instances only. It is deliberately test-local:
// there is no shared outer gate, overflow routing, borrowing, or automatic retry.
type isolationPools struct {
	high, low *airlock.Airlock
}

func newIsolationPools(t *testing.T, split bool, highQueue int) isolationPools {
	t.Helper()
	newPool := func(cfg airlock.Config) *airlock.Airlock {
		a, err := airlock.New(cfg)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			s := a.Stats()
			if s.InUse != 0 || s.Queued != 0 || s.ActivePermits != 0 || s.WaitingTenants != 0 {
				t.Errorf("leaked pool state: %+v", s)
			}
		})
		return a
	}
	const lowQueue = 128
	policy := &airlock.PriorityPolicy{
		Groups: []airlock.PriorityGroup{
			{Name: "H", Turns: 8, MaxQueued: highQueue},
			{Name: "M", Turns: 3, MaxQueued: 0},
			{Name: "L", Turns: 1, MaxQueued: lowQueue},
		},
		DefaultGroup: "L",
		TenantGroups: map[string]string{"H0": "H", "H1": "H", "M0": "M"},
	}
	if !split {
		a := newPool(airlock.Config{Capacity: isolationTotal, MaxQueued: highQueue + lowQueue,
			MaxQueuedPerTenant: lowQueue, Priority: policy})
		return isolationPools{high: a, low: a}
	}

	high := newPool(airlock.Config{Capacity: isolationHigh, MaxQueued: highQueue, MaxQueuedPerTenant: highQueue})
	policy.Groups = policy.Groups[1:]
	policy.TenantGroups = map[string]string{"M0": "M"}
	low := newPool(airlock.Config{Capacity: isolationLow, MaxQueued: lowQueue,
		MaxQueuedPerTenant: lowQueue, Priority: policy})
	return isolationPools{high: high, low: low}
}

func (p isolationPools) pool(high bool) *airlock.Airlock {
	if high {
		return p.high
	}
	return p.low
}

func isolationWaitQueued(t *testing.T, a *airlock.Airlock, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)

	for a.Stats().Queued != n {
		if time.Now().After(deadline) {
			t.Fatalf("queue did not reach %d: %+v", n, a.Stats())
		}

		runtime.Gosched()
	}
}

func isolationHold(t *testing.T, a *airlock.Airlock, tenant string, n int) *airlock.Permit {
	t.Helper()
	p, err := a.TryAcquireNFor(tenant, n)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(p.Release)
	return p
}

// The queue barrier establishes a selected L request while capacity remains.
// TryAcquire observes the invariant directly; no latency threshold is asserted.
func TestIsolationProtectedLowHead(t *testing.T) {
	for _, split := range []bool{false, true} {
		name := "shared"
		if split {
			name = "split"
		}

		t.Run(name, func(t *testing.T) {
			pools := newIsolationPools(t, split, 16)
			hold := isolationHold(t, pools.low, "L0", 7)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				p, err := pools.low.AcquireNFor(ctx, "L1", 8)
				if p != nil {
					p.Release()
				}

				done <- err
			}()
			isolationWaitQueued(t, pools.low, 1)
			high, err := pools.high.TryAcquireFor("H0")
			if split {
				if err != nil {
					t.Fatalf("low head blocked independent high pool: %v", err)
				}

				high.Release()
			} else if !errors.Is(err, airlock.ErrWouldBlock) {
				if high != nil {
					high.Release()
				}

				t.Fatalf("shared high bypassed protected low: %v", err)
			}

			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("waiting low cancellation: %v", err)
			}

			hold.Release()
		})
	}
}

func TestIsolationHighOverloadAndCancellation(t *testing.T) {
	pools := newIsolationPools(t, true, 1)
	hold := isolationHold(t, pools.high, "H0", isolationHigh)
	// All low capacity is usable while high is full, and the sum stays at 12.
	low := isolationHold(t, pools.low, "L0", isolationLow)
	if got := pools.high.Stats().InUse + pools.low.Stats().InUse; got != isolationTotal {
		t.Fatalf("used = %d", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		p, err := pools.high.AcquireFor(ctx, "H1")
		if p != nil {
			p.Release()
		}

		done <- err
	}()
	isolationWaitQueued(t, pools.high, 1)
	if p, err := pools.high.AcquireFor(ctx, "H0"); !errors.Is(err, airlock.ErrQueueFull) {
		if p != nil {
			p.Release()
		}

		t.Fatalf("full high queue: %v", err)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter: %v", err)
	}
	if s := pools.high.Stats(); s.Queued != 0 || s.InUse != isolationHigh {
		t.Fatalf("cancel changed holder or retained waiter: %+v", s)
	}

	hold.Release()
	if s := pools.high.Stats(); s.Queued != 0 || s.InUse != 0 {
		t.Fatalf("cancelled request retried or leaked: %+v", s)
	}

	low.Release()
}

func TestIsolationAcquisitionContextDoesNotRevokePermit(t *testing.T) {
	pools := newIsolationPools(t, true, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := pools.high.AcquireNFor(ctx, "H0", isolationHigh)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release()
	cancel()
	if other, err := pools.high.TryAcquireFor("H1"); !errors.Is(err, airlock.ErrWouldBlock) {
		if other != nil {
			other.Release()
		}

		t.Fatalf("cancellation reclaimed a live holder: %v", err)
	}

	p.Release()
	p.Release()
	next := isolationHold(t, pools.high, "H1", isolationHigh)
	next.Release()
}

func TestIsolationWeightBoundaryAndNoBorrowing(t *testing.T) {
	pools := newIsolationPools(t, true, 1)

	for _, high := range []bool{false, true} {
		a := pools.pool(high)
		if p, err := a.TryAcquireN(a.Stats().Capacity + 1); !errors.Is(err, airlock.ErrWeightExceedsCapacity) {
			if p != nil {
				p.Release()
			}

			t.Fatalf("oversize partition request: %v", err)
		}
	}

	low := isolationHold(t, pools.low, "L0", isolationLow)
	if p, err := pools.low.TryAcquireFor("L1"); !errors.Is(err, airlock.ErrWouldBlock) {
		if p != nil {
			p.Release()
		}

		t.Fatalf("low borrowed high capacity: %v", err)
	}

	high := isolationHold(t, pools.high, "H0", isolationHigh)
	high.Release()
	low.Release()
}

package comparison

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/tr1v3r/airlock"
)

type priorityArrival struct {
	group, tenant string
	weight        int
	arrival, hold time.Duration
	deadline      time.Duration
}

type priorityAdmission func(context.Context, string, int) (func(), error)

func priorityAdapter(t *testing.T, name string, capacity int) priorityAdmission {
	t.Helper()
	if name == "fifo" || name == "nested" {
		global := semaphore.NewWeighted(int64(capacity))
		locals := make(map[string]*semaphore.Weighted)
		for _, group := range []string{"H", "M", "L"} {
			for i := 0; i < 2; i++ {
				locals[fmt.Sprintf("%s%d", group, i)] = semaphore.NewWeighted(int64(capacity))
			}
		}
		return func(ctx context.Context, tenant string, n int) (func(), error) {
			local := locals[tenant]
			if name == "nested" {
				if err := local.Acquire(ctx, int64(n)); err != nil {
					return nil, err
				}
			}
			if err := global.Acquire(ctx, int64(n)); err != nil {
				if name == "nested" {
					local.Release(int64(n))
				}
				return nil, err
			}
			return func() {
				global.Release(int64(n))
				if name == "nested" {
					local.Release(int64(n))
				}
			}, nil
		}
	}
	cfg := airlock.Config{Capacity: capacity, MaxQueued: 576, MaxQueuedPerTenant: 192}
	if name == "priority" {
		cfg.Priority = &airlock.PriorityPolicy{
			Groups: []airlock.PriorityGroup{
				{Name: "H", Turns: 8, MaxQueued: 192},
				{Name: "M", Turns: 3, MaxQueued: 192},
				{Name: "L", Turns: 1, MaxQueued: 192},
			},
			DefaultGroup: "L",
			TenantGroups: map[string]string{"H0": "H", "H1": "H", "M0": "M", "M1": "M", "L0": "L", "L1": "L"},
		}
	}
	a, err := airlock.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s := a.Stats(); s.InUse != 0 || s.Queued != 0 || s.ActivePermits != 0 {
			t.Errorf("leaked admission state: %+v", s)
		}
	})
	return func(ctx context.Context, tenant string, n int) (func(), error) {
		p, err := a.AcquireNFor(ctx, tenant, n)
		if err != nil {
			return nil, err
		}
		return p.Release, nil
	}
}

func priorityTrace(scenario string, seed int64) []priorityArrival {
	rng := rand.New(rand.NewSource(seed))
	trace := make([]priorityArrival, 192)
	counts := map[string]int{}
	for i := range trace {
		group := []string{"H", "M", "L"}[i%3]
		if scenario == "sparse-low" {
			group = "H"
			if i%4 == 2 {
				group = "M"
			}
			if i%24 == 23 {
				group = "L"
			}
		}
		weight := 1
		hold := time.Duration(2+rng.Intn(3)) * time.Millisecond
		if scenario == "low-large" {
			switch group {
			case "L":
				weight, hold = 8, 6*time.Millisecond
			case "M":
				weight = 2
			}
		}
		if scenario == "cancel-mixed" {
			weight = 1 + rng.Intn(4)
		}
		j := priorityArrival{group: group, tenant: fmt.Sprintf("%s%d", group, counts[group]%2), weight: weight,
			arrival: time.Duration(i) * 100 * time.Microsecond, hold: hold}
		if scenario == "cancel-mixed" && rng.Intn(3) == 0 {
			j.deadline = time.Duration(3+rng.Intn(8)) * time.Millisecond
		}
		counts[group]++
		trace[i] = j
	}
	return trace
}

type prioritySample struct {
	group, outcome string
	wait           time.Duration
	grant          int
}

// Meter observes caller-visible intervals, not the internal reservation instant.
// Its idle-with-pending metric includes wakeup/return scheduling overhead and
// nested tenant waits; it is an upper-bound proxy, not exact scheduler HOL time.
type priorityMeter struct {
	mu                      sync.Mutex
	last                    time.Time
	used, pending, sequence int
	tokenTime, idlePending  time.Duration
}

func (m *priorityMeter) event(pendingDelta, usedDelta int, grant bool) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	d := now.Sub(m.last)
	m.tokenTime += time.Duration(m.used) * d
	if m.pending > 0 && m.used < 8 {
		m.idlePending += d
	}
	m.last = now
	m.pending += pendingDelta
	m.used += usedDelta
	if grant {
		m.sequence++
	}
	return m.sequence
}

// Run with AIRLOCK_PRIORITY_LOAD=1. This is a real public-API workload with a
// common precomputed arrival/weight/hold/deadline trace, not a CI latency gate.
func TestPriorityLoadComparison(t *testing.T) {
	if os.Getenv("AIRLOCK_PRIORITY_LOAD") != "1" {
		t.Skip("set AIRLOCK_PRIORITY_LOAD=1 for priority workload measurements")
	}
	for _, scenario := range []string{"saturated", "sparse-low", "low-large", "cancel-mixed"} {
		for seed := int64(1); seed <= 3; seed++ {
			trace := priorityTrace(scenario, seed)
			for _, name := range []string{"fifo", "nested", "airlock", "priority"} {
				t.Run(fmt.Sprintf("%s/seed%d/%s", scenario, seed, name), func(t *testing.T) {
					admit := priorityAdapter(t, name, 8)
					start := time.Now()
					meter := priorityMeter{last: start}
					results := make([]prioritySample, len(trace))
					var wg sync.WaitGroup
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					for i, j := range trace {
						if d := time.Until(start.Add(j.arrival)); d > 0 {
							time.Sleep(d)
						}
						wg.Add(1)
						go func(i int, j priorityArrival) {
							defer wg.Done()
							requestCtx := ctx
							if j.deadline != 0 {
								var done context.CancelFunc
								requestCtx, done = context.WithDeadline(ctx, start.Add(j.arrival+j.deadline))
								defer done()
							}
							begin := time.Now()
							meter.event(1, 0, false)
							release, err := admit(requestCtx, j.tenant, j.weight)
							r := prioritySample{group: j.group, wait: time.Since(begin)}
							if err != nil {
								meter.event(-1, 0, false)
								r.outcome = "rejected"
								if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
									r.outcome = "cancelled"
								}
							} else {
								r.grant = meter.event(-1, j.weight, true)
								time.Sleep(j.hold)
								meter.event(0, -j.weight, false)
								release()
								r.outcome = "granted"
							}
							results[i] = r
						}(i, j)
					}
					wg.Wait()
					meter.event(0, 0, false)
					elapsed := time.Since(start)
					total := 0
					for _, group := range []string{"H", "M", "L"} {
						var waits []time.Duration
						rejected, cancelled, first48 := 0, 0, 0
						for _, r := range results {
							if r.group != group {
								continue
							}
							switch r.outcome {
							case "granted":
								waits = append(waits, r.wait)
								if r.grant <= 48 {
									first48++
								}
							case "rejected":
								rejected++
							case "cancelled":
								cancelled++
							}
						}
						sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
						q := func(p int) float64 {
							if len(waits) == 0 {
								return 0
							}
							return float64(waits[(len(waits)-1)*p/100]) / float64(time.Millisecond)
						}
						total += len(waits)
						t.Logf("group=%s granted=%d rejected=%d cancelled=%d first48=%d wait_ms_p50=%.3f p95=%.3f p99=%.3f max=%.3f", group, len(waits), rejected, cancelled, first48, q(50), q(95), q(99), q(100))
						if rejected != 0 {
							t.Errorf("unexpected rejection with queue budgets >= offered requests: group=%s rejected=%d", group, rejected)
						}
					}
					t.Logf("throughput=%.1f/s utilization_proxy=%.4f idle_pending_proxy_ms=%.3f elapsed_ms=%.3f", float64(total)/elapsed.Seconds(), float64(meter.tokenTime)/float64(8*elapsed), float64(meter.idlePending)/float64(time.Millisecond), float64(elapsed)/float64(time.Millisecond))
					if scenario != "cancel-mixed" && total != len(trace) {
						t.Errorf("non-cancelling trace granted=%d, want %d", total, len(trace))
					}
					if meter.used != 0 || meter.pending != 0 {
						t.Fatalf("meter leaked: used=%d pending=%d", meter.used, meter.pending)
					}
				})
			}
		}
	}
}

// Strict priority is a policy counterexample, not a semaphore implementation or
// a finite-run proof of infinite starvation. Every selected group is replenished
// before the next slot, so the three group queues remain continuously nonempty.
func TestPriorityStrictStarvationCounterexample(t *testing.T) {
	const slots = 1200
	for _, policy := range []string{"strict", "8:3:1"} {
		pending := [3]int{1, 1, 1}
		granted, last, gap := [3]int{}, [3]int{-1, -1, -1}, [3]int{}
		cycle := []int{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2}
		cursor := 0
		for i := 0; i < slots; i++ {
			selected := -1
			if policy == "strict" {
				for group, queued := range pending {
					if queued > 0 {
						selected = group
						break
					}
				}
			} else {
				for range cycle {
					group := cycle[cursor]
					cursor = (cursor + 1) % len(cycle)
					if pending[group] > 0 {
						selected = group
						break
					}
				}
			}
			if selected < 0 {
				t.Fatal("no selection with nonempty queues")
			}
			pending[selected]--
			granted[selected]++
			if last[selected] >= 0 && i-last[selected]-1 > gap[selected] {
				gap[selected] = i - last[selected] - 1
			}
			last[selected] = i
			pending[selected]++
		}
		if policy == "strict" {
			if granted != [3]int{slots, 0, 0} {
				t.Fatalf("strict grants=%v", granted)
			}
		} else if granted != [3]int{800, 300, 100} || gap != [3]int{4, 9, 11} {
			t.Fatalf("weighted grants=%v max intervening selections=%v", granted, gap)
		}
		t.Logf("model=%s slots=%d grants(H/M/L)=%v max_intervening_selections=%v (gap undefined for never-selected groups)", policy, slots, granted, gap)
	}
}

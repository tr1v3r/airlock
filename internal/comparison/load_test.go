package comparison

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
	"golang.org/x/sync/semaphore"
)

type admission func(context.Context, string) (func(), error)

func adapters(capacity, queue int) map[string]admission {
	p := newChannelBaseline(capacity)
	s := semaphore.NewWeighted(int64(capacity))
	g, _ := airlock.New(airlock.Config{Capacity: capacity, MaxQueued: queue, MaxQueuedPerTenant: queue})
	m := map[string]admission{
		"channel": func(ctx context.Context, _ string) (func(), error) { err := p.Acquire(ctx); return p.Release, err },
		"weighted": func(ctx context.Context, _ string) (func(), error) {
			err := s.Acquire(ctx, 1)
			return func() { s.Release(1) }, err
		},
		"airlock": func(ctx context.Context, tenant string) (func(), error) {
			p, err := g.Acquire(ctx, tenant)
			if err != nil {
				return nil, err
			}
			return p.Release, nil
		},
	}
	for _, limit := range []int{1, capacity / 2, capacity} {
		global := semaphore.NewWeighted(int64(capacity))
		locals := map[string]*semaphore.Weighted{"A": semaphore.NewWeighted(int64(limit)), "B": semaphore.NewWeighted(int64(limit))}
		m[fmt.Sprintf("nested-%d", limit)] = func(ctx context.Context, tenant string) (func(), error) {
			return acquireNested(ctx, global, locals[tenant])
		}
	}
	return m
}

// Opt-in actual goroutine workload, not a CI timing assertion. All adapters get
// the same outer in-system budget (executing + pending), avoiding a hidden
// unlimited backlog advantage. Airlock's own queue limit is also configured.
func TestLoadComparison(t *testing.T) {
	if os.Getenv("AIRLOCK_LOAD") != "1" {
		t.Skip("set AIRLOCK_LOAD=1 for workload measurements")
	}
	for _, scenario := range []string{"single", "flood", "mixed", "overload"} {
		for seed := int64(1); seed <= 3; seed++ {
			queue := 256
			if scenario == "overload" {
				queue = 12
			}
			all := adapters(4, queue)
			for _, name := range []string{"channel", "weighted", "nested-1", "nested-2", "nested-4", "airlock"} {
				t.Run(fmt.Sprintf("%s/seed%d/%s", scenario, seed, name), func(t *testing.T) {
					rng := rand.New(rand.NewSource(seed))
					type sample struct {
						tenant  string
						wait    time.Duration
						outcome string
					}
					const count = 96
					results := make([]sample, count)
					services := make([]time.Duration, count)
					for i := range services {
						services[i] = time.Millisecond
						if scenario == "mixed" && rng.Intn(4) == 0 {
							services[i] = 10 * time.Millisecond
						}
					}
					budget := make(chan struct{}, 4+queue)
					var wg sync.WaitGroup
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					start := time.Now()
					for i := 0; i < count; i++ {
						// Open-loop fixed arrival trace: A flood, sparse B every 12 arrivals.
						arrival := start.Add(time.Duration(i) * 100 * time.Microsecond)
						if d := time.Until(arrival); d > 0 {
							time.Sleep(d)
						}
						tenant := "A"
						if scenario != "single" && i%12 == 11 {
							tenant = "B"
						}
						wg.Add(1)
						go func(i int, tenant string) {
							defer wg.Done()
							r := sample{tenant: tenant}
							begin := time.Now()
							select {
							case budget <- struct{}{}:
							default:
								r.outcome = "rejected"
								results[i] = r
								return
							}
							defer func() { <-budget }()
							release, err := all[name](ctx, tenant)
							r.wait = time.Since(begin)
							if err != nil {
								if ctx.Err() != nil {
									r.outcome = "cancelled"
								} else {
									r.outcome = "rejected"
								}
								results[i] = r
								return
							}
							time.Sleep(services[i])
							release()
							r.outcome = "completed"
							results[i] = r
						}(i, tenant)
					}
					wg.Wait()
					elapsed := time.Since(start)
					runtime.ReadMemStats(&after)
					completed := 0
					for _, tenant := range []string{"A", "B"} {
						var waits []time.Duration
						rejected, cancelled := 0, 0
						for _, r := range results {
							if r.tenant != tenant {
								continue
							}
							switch r.outcome {
							case "completed":
								waits = append(waits, r.wait)
							case "rejected":
								rejected++
							case "cancelled":
								cancelled++
							}
						}
						sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
						q := func(p int) time.Duration {
							if len(waits) == 0 {
								return 0
							}
							return waits[(len(waits)-1)*p/100]
						}
						completed += len(waits)
						t.Logf("tenant=%s completed=%d rejected=%d cancelled=%d wait_p50=%s wait_p95=%s wait_p99=%s", tenant, len(waits), rejected, cancelled, q(50), q(95), q(99))
					}
					t.Logf("throughput=%.1f/s allocs=%d allocated_bytes=%d heap_delta=%d", float64(completed)/elapsed.Seconds(), after.Mallocs-before.Mallocs, after.TotalAlloc-before.TotalAlloc, int64(after.HeapAlloc)-int64(before.HeapAlloc))
					if len(budget) != 0 {
						t.Fatal("in-system budget leaked")
					}
				})
			}
		}
	}
}

func BenchmarkAdmission(b *testing.B) {
	for name, a := range adapters(4, 256) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				release, err := a(context.Background(), "A")
				if err != nil {
					b.Fatal(err)
				}
				release()
			}
		})
	}
}

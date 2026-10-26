package comparison

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

type isolationArrival struct {
	high                  bool
	tenant                string
	weight                int
	arrival, hold, budget time.Duration
}

func isolationTrace(scenario string, seed int64) []isolationArrival {
	rng := rand.New(rand.NewSource(seed))
	lowCount, highCount := 96, 64
	if scenario == "low-large" || scenario == "low-long" {
		lowCount = 32
	}

	if scenario == "high-overload" {
		lowCount, highCount = 16, 160
	}

	if scenario == "low-only" {
		highCount = 0
	}

	trace := make([]isolationArrival, 0, lowCount+highCount)

	for i := 0; i < lowCount; i++ {
		weight := 1
		hold := time.Duration(4+rng.Intn(3)) * time.Millisecond

		switch scenario {
		case "low-large":
			weight, hold = 8, time.Duration(8+rng.Intn(5))*time.Millisecond
		case "low-long":
			hold = time.Duration(40+rng.Intn(21)) * time.Millisecond
		}

		trace = append(trace, isolationArrival{tenant: fmt.Sprintf("L%d", i%2), weight: weight, hold: hold})
	}

	for i := 0; i < highCount; i++ {
		hold := time.Duration(1+rng.Intn(2)) * time.Millisecond
		interval := time.Millisecond
		if scenario == "high-overload" {
			hold, interval = time.Duration(8+rng.Intn(5))*time.Millisecond, 50*time.Microsecond
		}

		trace = append(trace, isolationArrival{high: true, tenant: fmt.Sprintf("H%d", i%2), weight: 1,
			arrival: 10*time.Millisecond + time.Duration(i)*interval, hold: hold, budget: 5 * time.Millisecond})
	}

	return trace
}

type isolationSample struct {
	high, lateGrant, expiredAtCall bool
	outcome                        string
	wait, lag                      time.Duration
}

// Measures returned-but-not-yet-releasing permits. Internal grant time and the
// start of resource use are not observable through the public API. This is a
// token-time proxy, not CPU utilization or an exact admission accounting trace.
type isolationMeter struct {
	mu        sync.Mutex
	last      time.Time
	used      [2]int
	tokenTime [2]time.Duration
}

func (m *isolationMeter) event(high bool, delta int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()

	for i, used := range m.used {
		m.tokenTime[i] += time.Duration(used) * now.Sub(m.last)
	}

	m.last = now
	index := 1
	if high {
		index = 0
	}

	m.used[index] += delta
}

type isolationClassResult struct {
	Submitted        int      `json:"submitted"`
	Granted          int      `json:"granted"`
	Rejected         int      `json:"rejected"`
	TimedOut         int      `json:"timed_out"`
	LateGrants       int      `json:"late_grants"`
	ExpiredAtCall    int      `json:"expired_at_call"`
	SuccessP95MS     *float64 `json:"success_p95_ms"`
	SuccessP99MS     *float64 `json:"success_p99_ms"`
	SuccessMaxMS     *float64 `json:"success_max_ms"`
	AllResponseP99MS *float64 `json:"all_response_p99_ms"`
	ArrivalLagP99MS  *float64 `json:"arrival_lag_p99_ms"`
}

type isolationLoadResult struct {
	Scenario        string               `json:"scenario"`
	Seed            int64                `json:"seed"`
	Mode            string               `json:"mode"`
	High            isolationClassResult `json:"high"`
	Low             isolationClassResult `json:"low"`
	ElapsedMS       float64              `json:"elapsed_ms"`
	Throughput      float64              `json:"throughput"`
	Utilization     float64              `json:"utilization_pct"`
	ReservedIdlePct *float64             `json:"reserved_idle_pct"`
	HighTokenTimeMS float64              `json:"high_token_time_ms"`
	LowTokenTimeMS  float64              `json:"low_token_time_ms"`
}

func isolationQuantile(values []time.Duration, percentile int) *float64 {
	if len(values) == 0 {
		return nil
	}

	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	v := float64(values[(len(values)-1)*percentile/100]) / float64(time.Millisecond)
	return &v
}

func isolationSummarize(t *testing.T, samples []isolationSample, high bool) isolationClassResult {
	t.Helper()
	var result isolationClassResult
	var successful, all, lag []time.Duration

	for _, sample := range samples {
		if sample.high != high {
			continue
		}

		result.Submitted++
		all, lag = append(all, sample.wait), append(lag, sample.lag)
		if sample.expiredAtCall {
			result.ExpiredAtCall++
		}

		if sample.lateGrant {
			result.LateGrants++
		}

		switch sample.outcome {
		case "granted":
			result.Granted++
			successful = append(successful, sample.wait)
		case "queue-full":
			result.Rejected++
		case "deadline":
			result.TimedOut++
		default:
			t.Errorf("unexpected acquisition outcome: %s", sample.outcome)
		}
	}

	result.SuccessP95MS = isolationQuantile(successful, 95)
	result.SuccessP99MS = isolationQuantile(successful, 99)
	result.SuccessMaxMS = isolationQuantile(successful, 100)
	result.AllResponseP99MS = isolationQuantile(all, 99)
	result.ArrivalLagP99MS = isolationQuantile(lag, 99)
	if !high && result.Granted != result.Submitted {
		t.Errorf("low queue should not reject or time out: %+v", result)
	}
	return result
}

func runIsolationLoad(t *testing.T, scenario string, seed int64, split bool) isolationLoadResult {
	t.Helper()
	pools := newIsolationPools(t, split, 16)
	trace := isolationTrace(scenario, seed)
	start := time.Now().Add(5 * time.Millisecond)
	meter := isolationMeter{last: start}
	samples := make([]isolationSample, len(trace))
	outerCtx, outerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer outerCancel()
	var wg sync.WaitGroup

	for i, request := range trace {
		wg.Add(1)
		go func(i int, request isolationArrival) {
			defer wg.Done()
			planned := start.Add(request.arrival)
			if delay := time.Until(planned); delay > 0 {
				time.Sleep(delay)
			}

			ctx := outerCtx
			deadline := planned.Add(request.budget)
			if request.budget > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(outerCtx, deadline)
				defer cancel()
			}

			begin := time.Now()
			sample := isolationSample{high: request.high, lag: begin.Sub(planned),
				expiredAtCall: request.budget > 0 && !begin.Before(deadline)}
			p, err := pools.pool(request.high).AcquireNFor(ctx, request.tenant, request.weight)
			returned := time.Now()
			sample.wait = returned.Sub(begin)

			switch {
			case err == nil:
				sample.outcome = "granted"
				sample.lateGrant = request.budget > 0 && returned.After(deadline)
				meter.event(request.high, request.weight)
				// Acquisition deadlines do not revoke active work. This bounded
				// simulated task retains its permit until it actually finishes.
				time.Sleep(request.hold)
				meter.event(request.high, -request.weight)
				p.Release()

			case errors.Is(err, context.DeadlineExceeded):
				sample.outcome = "deadline"
			case errors.Is(err, airlock.ErrQueueFull), errors.Is(err, airlock.ErrGroupQueueFull), errors.Is(err, airlock.ErrTenantQueueFull):
				sample.outcome = "queue-full"
			default:
				sample.outcome = fmt.Sprintf("unexpected: %v", err)
			}

			samples[i] = sample
		}(i, request)
	}

	wg.Wait()
	meter.event(false, 0)
	if err := outerCtx.Err(); err != nil {
		t.Fatalf("experiment timed out: %v", err)
	}

	elapsed := time.Since(start)
	result := isolationLoadResult{Scenario: scenario, Seed: seed, Mode: "shared",
		High: isolationSummarize(t, samples, true), Low: isolationSummarize(t, samples, false),
		ElapsedMS:       float64(elapsed) / float64(time.Millisecond),
		Utilization:     100 * float64(meter.tokenTime[0]+meter.tokenTime[1]) / (isolationTotal * float64(elapsed)),
		HighTokenTimeMS: float64(meter.tokenTime[0]) / float64(time.Millisecond),
		LowTokenTimeMS:  float64(meter.tokenTime[1]) / float64(time.Millisecond)}
	result.Throughput = float64(result.High.Granted+result.Low.Granted) / elapsed.Seconds()
	if split {
		result.Mode = "split"
		idle := 100 * (1 - float64(meter.tokenTime[0])/(isolationHigh*float64(elapsed)))
		result.ReservedIdlePct = &idle
	}
	return result
}

// Opt in to wall-clock experiments. Correctness CI uses the deterministic
// TestIsolation* tests; no percentile or relative-speed assertion is made here.
func TestIsolationLoadComparison(t *testing.T) {
	if os.Getenv("AIRLOCK_ISOLATION_LOAD") != "1" {
		t.Skip("set AIRLOCK_ISOLATION_LOAD=1 for capacity isolation experiments")
	}

	t.Logf("platform=%s/%s go=%s GOMAXPROCS=%d total=%d high=%d low=%d highQueue=16 lowQueue=128 highWaitBudget=5ms",
		runtime.GOOS, runtime.GOARCH, runtime.Version(), runtime.GOMAXPROCS(0), isolationTotal, isolationHigh, isolationLow)

	for _, scenario := range []string{"low-flood", "low-large", "low-long", "high-overload", "low-only"} {
		for seed := int64(1); seed <= 3; seed++ {
			// Alternate adapter order across seeds to reduce a fixed order bias.
			for _, split := range []bool{seed%2 == 0, seed%2 != 0} {
				name := "shared"
				if split {
					name = "split"
				}

				t.Run(fmt.Sprintf("%s/seed%d/%s", scenario, seed, name), func(t *testing.T) {
					result := runIsolationLoad(t, scenario, seed, split)
					data, err := json.Marshal(result)
					if err != nil {
						t.Fatal(err)
					}

					t.Logf("ISOLATION_RESULT %s", data)
				})
			}
		}
	}
}

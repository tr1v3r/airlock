// Command mixedload validates a closed-loop Driver workload on disposable services.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	redis "github.com/redis/go-redis/v9"
	a "github.com/tr1v3r/airlock"
	pg "github.com/tr1v3r/airlock/driver/postgres"
	rd "github.com/tr1v3r/airlock/driver/redis"
)

var errInvalidGrant = errors.New("invalid granted response")

const ttl = 2 * time.Second
const timeout = 500 * time.Millisecond
const hold = 600 * time.Millisecond
const renewEvery = 200 * time.Millisecond
const pollEvery = 50 * time.Millisecond

type config struct {
	Backend, Address, Scenario, Output string
	History, Capacity, Workers, Repeat int
	Duration                           time.Duration
}
type sample struct {
	Op, Phase, Status, Error, ErrorClass string
	StartMS, EndMS, LatencyMS            float64
	BudgetMS, PriorBudgetMS              *float64
	Unusable                             bool
}
type dist struct {
	N             int
	Min, P50, Max float64
	P95, P99      *float64
}
type population struct {
	Records, Granted, Queued, Terminal, Bytes, RowBytes, IndexBytes int64
	SeedHistory, SeedActive, SeedQueued, SeedExpired                int64
}
type report struct {
	Type                                                         string
	Backend, Scenario                                            string
	History, Capacity, Workers, Repeat                           int
	DurationSeconds                                              float64
	Operations, Successes, Admissions                            int
	OperationsPerSecond, SuccessesPerSecond, AdmissionsPerSecond float64
	Latencies, Budgets                                           map[string]dist
	PriorRenewBudget                                             dist
	Errors, Statuses                                             map[string]int
	AllStatuses                                                  map[string]int
	AllErrors                                                    map[string]int
	BoundaryOperations, BoundaryErrors                           int
	InvariantErrors                                              []string
	UnusableGrants, CleanupErrors                                int
	Before, After                                                population
	HeapBefore, HeapAfter, PeakSampledHeap, Allocated            uint64
	GCs                                                          uint32
	GoVersion                                                    string
	GOMAXPROCS                                                   int
	OffsetMS, ClockRTTMS, ExpiryBoundaryMS, SetupSeconds         float64
	Settings                                                     map[string]float64
}
type backend struct {
	d                                a.Driver
	r                                *redis.Client
	db                               *sql.DB
	namespace, key, resource, rt, qt string
	seedRoles                        map[string]string
}

func main() {
	var c config
	flag.StringVar(&c.Backend, "backend", "redis", "redis/postgres")
	flag.StringVar(&c.Address, "address", "", "disposable service address/DSN")
	flag.StringVar(&c.Scenario, "scenario", "steady", "steady/expiry-gc")
	flag.StringVar(&c.Output, "output", "", "JSONL output required")
	flag.IntVar(&c.History, "history", 0, "terminal history fixture size")
	flag.IntVar(&c.Capacity, "capacity", 8, "capacity")
	flag.IntVar(&c.Workers, "workers", 8, "lifecycle workers")
	flag.IntVar(&c.Repeat, "repeat", 1, "trial number")
	flag.DurationVar(&c.Duration, "duration", 15*time.Second, "measurement window")
	flag.Parse()
	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
func earlier(x, y time.Time) time.Time {
	if x.Before(y) {
		return x
	}
	return y
}
func sleep(t time.Time) {
	if d := time.Until(t); d > 0 {
		time.Sleep(d)
	}
}
func errorClass(err error) string {
	switch {
	case errors.Is(err, errInvalidGrant):
		return "invalid_grant"
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, a.ErrLeaseLost):
		return "lease_lost"
	case errors.Is(err, a.ErrQueueFull) || errors.Is(err, a.ErrTenantQueueFull):
		return "queue_full"
	case errors.Is(err, a.ErrSubmitExpired):
		return "submit_expired"
	default:
		return "other"
	}
}
func run(c config) error {
	if c.Address == "" || c.Output == "" || c.History < 0 || c.Capacity < 1 || c.Workers < 1 || c.Duration <= 0 || (c.Scenario != "steady" && c.Scenario != "expiry-gc") {
		return errors.New("invalid settings")
	}
	setup := time.Now()
	b, err := connect(c)
	if err != nil {
		return err
	}
	defer b.close()
	ctx := context.Background()
	t0 := time.Now()
	now, err := b.now(ctx)
	t1 := time.Now()
	if err != nil {
		return err
	}
	offset := now.Sub(t0.Add(t1.Sub(t0) / 2))
	if offset > 100*time.Millisecond || offset < -100*time.Millisecond {
		return fmt.Errorf("backend clock offset %s too large", offset)
	}
	policy := a.ResourceConfig{Admission: a.Config{Capacity: c.Capacity, MaxQueued: max(c.Workers*2, c.Capacity*3), MaxQueuedPerTenant: max(c.Workers*2, c.Capacity*3)}, LeaseTTL: ttl, MaxWait: 10 * time.Second, SubmitWindow: 5 * time.Second, Retention: 60 * time.Second, MaxRecords: 100000}
	state, err := fixture(now, c, policy, b.resource)
	if err != nil {
		return err
	}
	if err = b.seed(ctx, state); err != nil {
		return err
	}
	// Exercise the normal mutation path to validate storage and build Redis projections.
	manager := b.d.(a.ResourceManager)
	if _, err = manager.Resize(ctx, a.ResizeRequest{Resource: b.resource, Capacity: c.Capacity, ExpectedVersion: state.CapacityVersion}); err != nil {
		return fmt.Errorf("warmup: %w", err)
	}
	before, err := b.stats(ctx)
	if err != nil {
		return err
	}
	start := time.Now()
	stop := start.Add(c.Duration)
	boundary := 0.0
	if c.Scenario == "expiry-gc" {
		boundary = ms(now.Add(ttl).Sub(start.Add(offset)))
		if boundary < 250 {
			return errors.New("fixture expired before measurement")
		}
	}
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	var peak atomic.Uint64
	peak.Store(m0.HeapAlloc)
	samplerStop := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-samplerStop:
				return
			case <-t.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				for old := peak.Load(); m.HeapAlloc > old; old = peak.Load() {
					if peak.CompareAndSwap(old, m.HeapAlloc) {
						break
					}
				}
			}
		}
	}()
	var mu sync.Mutex
	data := make([]sample, 0, 10000)
	var admitted atomic.Int64
	call := func(op, phase string, r a.Request, id string, prior time.Time) (a.Result, error, time.Time) {
		begin := time.Now()
		cx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		var out a.Result
		var e error
		switch op {
		case "submit":
			out, e = b.d.Submit(cx, r)
		case "observe":
			out, e = b.d.Observe(cx, r)
		case "renew":
			out, e = b.d.Renew(cx, a.LeaseRef{Request: r, ID: id})
		case "release":
			out, e = b.d.Release(cx, a.LeaseRef{Request: r, ID: id})
		case "cancel":
			out, e = b.d.Cancel(cx, r)
		}
		end := time.Now()
		if e == nil && out.Status == a.Granted && (out.LeaseID == "" || out.Weight != r.Weight || out.Remaining <= 0 || out.Remaining > ttl || (op == "renew" && out.LeaseID != id)) {
			e = errInvalidGrant
		}
		if phase == "measured" && !end.Before(stop) {
			phase = "boundary"
		}
		s := sample{Op: op, Phase: phase, StartMS: ms(begin.Sub(start)), EndMS: ms(end.Sub(start)), LatencyMS: ms(end.Sub(begin)), Status: string(out.Status)}
		if e != nil {
			s.Error = e.Error()
			s.ErrorClass = errorClass(e)
		}
		deadline := begin.Add(out.Remaining)
		if !prior.IsZero() {
			n := ms(prior.Sub(begin))
			s.PriorBudgetMS = &n
			if e == nil && !end.Before(prior) {
				s.Unusable = true
			}
		}
		if e == nil && out.Status == a.Granted {
			n := ms(deadline.Sub(end))
			s.BudgetMS = &n
			if n <= 0 {
				s.Unusable = true
			}
		}
		mu.Lock()
		data = append(data, s)
		mu.Unlock()
		return out, e, deadline
	}
	var wg sync.WaitGroup
	for worker := 0; worker < c.Workers; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for time.Now().Before(stop) {
				r := a.NewRequest(time.Now().Add(4 * time.Second))
				r.Resource = b.resource
				r.Config = policy
				r.Tenant = fmt.Sprintf("tenant-%d", w%8)
				r.Weight = 1
				r.WaitUntil = time.Now().Add(9 * time.Second).UTC().Truncate(time.Millisecond)
				out, e, deadline := call("submit", "measured", r, "", time.Time{})
				for e == nil && out.Status == a.Queued && time.Now().Before(stop) {
					sleep(earlier(stop, time.Now().Add(pollEvery)))
					if !time.Now().Before(stop) {
						break
					}
					out, e, deadline = call("observe", "measured", r, "", time.Time{})
				}
				if e != nil || out.Status != a.Granted || !time.Now().Before(deadline) {
					call("cancel", "cleanup", r, "", time.Time{})
					continue
				}
				if time.Now().Before(stop) {
					admitted.Add(1)
				}
				until := time.Now().Add(hold)
				live := true
				for time.Now().Before(until) && time.Now().Before(stop) {
					sleep(earlier(until, earlier(stop, time.Now().Add(renewEvery))))
					if !time.Now().Before(until) || !time.Now().Before(stop) {
						break
					}
					if !time.Now().Before(deadline) {
						live = false
						break
					}
					old := deadline
					next, e, end := call("renew", "measured", r, out.LeaseID, old)
					if e != nil || next.Status != a.Granted || !time.Now().Before(old) || !time.Now().Before(end) {
						live = false
						break
					}
					deadline = end
				}
				phase := "measured"
				if !time.Now().Before(stop) {
					phase = "cleanup"
				}
				if live {
					call("release", phase, r, out.LeaseID, time.Time{})
				} else {
					call("cancel", "cleanup", r, "", time.Time{})
				}
			}
		}(worker)
	}
	wg.Wait()
	close(samplerStop)
	<-samplerDone
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	after, err := b.stats(ctx)
	if err != nil {
		return err
	}
	result := summarize(c, data)
	result.Admissions = int(admitted.Load())
	result.AdmissionsPerSecond = float64(result.Admissions) / c.Duration.Seconds()
	result.Before = before
	result.After = after
	if before.SeedHistory != int64(c.History) {
		result.InvariantErrors = append(result.InvariantErrors, "seed history count mismatch")
	}
	if c.Scenario == "steady" && after.SeedHistory != int64(c.History) {
		result.InvariantErrors = append(result.InvariantErrors, "steady history was reclaimed unexpectedly")
	}
	if c.Scenario == "expiry-gc" && c.Duration > 3*time.Second && (after.SeedHistory != 0 || after.SeedExpired != int64(2*c.Capacity)) {
		result.InvariantErrors = append(result.InvariantErrors, "concentrated expiry/GC did not finish")
	}
	if after.Granted > int64(c.Capacity) {
		result.InvariantErrors = append(result.InvariantErrors, "granted record count exceeds capacity")
	}

	result.HeapBefore = m0.HeapAlloc
	result.HeapAfter = m1.HeapAlloc
	result.PeakSampledHeap = peak.Load()
	result.Allocated = m1.TotalAlloc - m0.TotalAlloc
	result.GCs = m1.NumGC - m0.NumGC
	result.GoVersion = runtime.Version()
	result.GOMAXPROCS = runtime.GOMAXPROCS(0)
	result.OffsetMS = ms(offset)
	result.ClockRTTMS = ms(t1.Sub(t0))
	result.ExpiryBoundaryMS = boundary
	result.SetupSeconds = start.Sub(setup).Seconds()
	result.Settings = map[string]float64{"ttl_ms": ms(ttl), "operation_timeout_ms": ms(timeout), "hold_ms": ms(hold), "renew_every_ms": ms(renewEvery), "poll_every_ms": ms(pollEvery), "retention_ms": ms(policy.Retention), "pool_size": float64(max(c.Workers, 16)), "max_records": float64(policy.MaxRecords), "max_queued": float64(policy.Admission.MaxQueued)}
	f, err := os.Create(c.Output)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, s := range data {
		if err = enc.Encode(s); err != nil {
			f.Close()
			return err
		}
	}
	if err = enc.Encode(result); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	out, _ := json.Marshal(result)
	fmt.Println(string(out))
	return nil
}
func describe(v []float64) dist {
	sort.Float64s(v)
	d := dist{N: len(v)}
	if len(v) == 0 {
		return d
	}
	at := func(p float64) float64 {
		i := int(float64(len(v)) * p)
		if i >= len(v) {
			i = len(v) - 1
		}
		return v[i]
	}
	d.Min = v[0]
	d.Max = v[len(v)-1]
	d.P50 = at(.5)
	if len(v) >= 20 {
		x := at(.95)
		d.P95 = &x
	}
	if len(v) >= 100 {
		x := at(.99)
		d.P99 = &x
	}
	return d
}
func summarize(c config, data []sample) report {
	r := report{Type: "summary", Backend: c.Backend, Scenario: c.Scenario, History: c.History, Capacity: c.Capacity, Workers: c.Workers, Repeat: c.Repeat, DurationSeconds: c.Duration.Seconds(), Latencies: map[string]dist{}, Budgets: map[string]dist{}, Errors: map[string]int{}, Statuses: map[string]int{}, AllStatuses: map[string]int{}, AllErrors: map[string]int{}}
	lat := map[string][]float64{}
	bud := map[string][]float64{}
	var prior []float64
	for _, s := range data {
		r.AllStatuses[s.Phase+"/"+s.Op+"/"+s.Status]++
		if s.Error != "" {
			r.AllErrors[s.Phase+"/"+s.Op+"/"+s.ErrorClass]++
		}
		if s.Phase == "boundary" {
			r.BoundaryOperations++
			if s.Error != "" {
				r.BoundaryErrors++
			}
			continue
		}
		if s.Phase != "measured" {
			if s.Error != "" {
				r.CleanupErrors++
			}
			continue
		}
		r.Operations++
		kind := "ok"
		if s.Error != "" {
			kind = "error"
			r.Errors[s.Op+"/"+s.ErrorClass]++
		} else {
			r.Successes++
		}
		lat[s.Op+"/"+kind] = append(lat[s.Op+"/"+kind], s.LatencyMS)
		r.Statuses[s.Op+"/"+s.Status]++
		if s.BudgetMS != nil {
			bud[s.Op] = append(bud[s.Op], *s.BudgetMS)
		}
		if s.PriorBudgetMS != nil {
			prior = append(prior, *s.PriorBudgetMS)
		}
		if s.Unusable {
			r.UnusableGrants++
		}
	}
	for k, v := range lat {
		r.Latencies[k] = describe(v)
	}
	for k, v := range bud {
		r.Budgets[k] = describe(v)
	}
	r.PriorRenewBudget = describe(prior)
	r.OperationsPerSecond = float64(r.Operations) / c.Duration.Seconds()
	r.SuccessesPerSecond = float64(r.Successes) / c.Duration.Seconds()
	return r
}
func connect(c config) (*backend, error) {
	b := &backend{namespace: "validation-" + a.NewRequest(time.Now()).Owner, resource: "capacity"}
	if c.Backend == "redis" {
		b.r = redis.NewClient(&redis.Options{Addr: c.Address, ContextTimeoutEnabled: true, PoolSize: max(c.Workers, 16)})
		d, err := rd.New(b.r, b.namespace)
		if err != nil {
			return nil, err
		}
		b.d = d
		b.key = fmt.Sprintf("airlock:{%x}:state", sha256.Sum256([]byte(b.namespace+"\x00"+b.resource)))
		return b, nil
	}
	if c.Backend != "postgres" {
		return nil, errors.New("unknown backend")
	}
	db, err := sql.Open("pgx", c.Address)
	if err != nil {
		return nil, err
	}
	b.db = db
	db.SetMaxOpenConns(max(c.Workers, 16))
	table := "mixed_" + a.NewRequest(time.Now()).Owner[:12]
	b.rt = "public." + table
	b.qt = b.rt + "_requests"
	d, err := pg.New(db, b.namespace, pg.WithTable(table))
	if err != nil {
		return nil, err
	}
	b.d = d
	if err = d.EnsureSchema(context.Background()); err != nil {
		return nil, err
	}
	return b, nil
}
func (b *backend) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if b.r != nil {
		_ = b.r.Del(ctx, b.key, strings.TrimSuffix(b.key, ":state")+":update-lock").Err()
		_ = b.r.Close()
	}
	if b.db != nil {
		_, _ = b.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+b.qt)
		_, _ = b.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+b.rt)
		_ = b.db.Close()
	}
}
func (b *backend) now(ctx context.Context) (time.Time, error) {
	if b.r != nil {
		return b.r.Time(ctx).Result()
	}
	var now time.Time
	err := b.db.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&now)
	return now, err
}
func fixture(now time.Time, c config, cfg a.ResourceConfig, resource string) (*a.SharedState, error) {
	now = now.UTC().Truncate(time.Millisecond)
	s := &a.SharedState{Version: 1, Resource: resource, Config: cfg, Capacity: cfg.Admission.Capacity, CapacityVersion: 1, Revision: 1, Time: now, Records: map[string]*a.DriverRecord{}, Queues: map[string][]string{}, Rotations: [][]string{{}}}
	for i := 0; i < c.History; i++ {
		r := a.NewRequest(now.Add(-time.Second))
		r.Resource = resource
		r.Weight = 1
		r.WaitUntil = r.SubmitUntil
		finished := now
		if c.Scenario == "expiry-gc" {
			finished = now.Add(-cfg.Retention + ttl)
		}
		s.Records[r.ID] = &a.DriverRecord{Request: r, Status: a.Cancelled, Finished: finished}
	}
	if c.Scenario == "expiry-gc" {
		for i := 0; i < c.Capacity*2; i++ {
			r := a.NewRequest(now.Add(4 * time.Second))
			r.Resource = resource
			r.Weight = 1
			r.WaitUntil = now.Add(ttl)
			r.Tenant = "fixture"
			rec := &a.DriverRecord{Request: r, Until: now.Add(ttl)}
			if i < c.Capacity {
				rec.Status = a.Granted
				h := sha256.Sum256([]byte(r.ID + ":" + r.Owner))
				rec.LeaseID = hex.EncodeToString(h[:16])
			} else {
				rec.Status = a.Queued
				s.Queues["fixture"] = append(s.Queues["fixture"], r.ID)
			}
			s.Records[r.ID] = rec
		}
		s.Rotations = [][]string{{"fixture"}}
	}
	next, _, _, err := a.Transition(s, now, a.Command{Kind: a.Resize, Resize: a.ResizeRequest{Resource: resource, Capacity: c.Capacity, ExpectedVersion: 1}})
	return next, err
}
func (b *backend) seed(ctx context.Context, s *a.SharedState) error {
	b.seedRoles = make(map[string]string, len(s.Records))
	for id, rec := range s.Records {
		if rec.Request.Tenant == "fixture" {
			b.seedRoles[id] = "active"
		} else {
			b.seedRoles[id] = "history"
		}
	}

	if b.r != nil {
		data, err := a.EncodeState(s)
		if err != nil {
			return err
		}
		return b.r.HSet(ctx, b.key, "state", string(data)).Err()
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	cfg, _ := json.Marshal(s.Config)
	rotations, _ := json.Marshal(s.Rotations)
	var maintenance any
	if t := s.NextMaintenance(); !t.IsZero() {
		maintenance = t.UnixMilli()
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+b.rt+" (namespace,resource,state_version,config,rotations,protected,cursor,revision,observed_ms,next_maintenance_ms,capacity,capacity_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", b.namespace, s.Resource, s.Version, string(cfg), string(rotations), s.Protected, s.Cursor, s.Revision, s.Time.UnixMilli(), maintenance, s.Capacity, s.CapacityVersion)
	if err != nil {
		return err
	}
	positions := map[string]int{}
	for _, queue := range s.Queues {
		for i, id := range queue {
			positions[id] = i
		}
	}
	args := make([]any, 0, 256*14)
	var values []string
	flush := func() error {
		if len(values) == 0 {
			return nil
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO "+b.qt+" (namespace,resource,id,owner,tenant,weight,submit_until_ms,wait_until_ms,status,lease_id,until_ms,finished_ms,failure,queue_position) VALUES "+strings.Join(values, ","), args...)
		args = args[:0]
		values = values[:0]
		return err
	}
	for id, r := range s.Records {
		var until, finished, position any
		if !r.Until.IsZero() {
			until = r.Until.UnixMilli()
		}
		if !r.Finished.IsZero() {
			finished = r.Finished.UnixMilli()
		}
		if n, ok := positions[id]; ok {
			position = n
		}
		place := make([]string, 14)
		for i := range place {
			place[i] = fmt.Sprintf("$%d", len(args)+i+1)
		}
		values = append(values, "("+strings.Join(place, ",")+")")
		args = append(args, b.namespace, s.Resource, id, r.Request.Owner, r.Request.Tenant, r.Request.Weight, r.Request.SubmitUntil.UnixMilli(), r.Request.WaitUntil.UnixMilli(), r.Status, r.LeaseID, until, finished, r.Failure, position)
		if len(values) == 256 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return tx.Commit()
}
func (b *backend) stats(ctx context.Context) (population, error) {
	var m population
	if b.r != nil {
		data, err := b.r.HGet(ctx, b.key, "state").Bytes()
		if err != nil {
			return m, err
		}
		s, err := a.DecodeState(data)
		if err != nil {
			return m, err
		}
		m.Records = int64(len(s.Records))
		for id, r := range s.Records {
			b.seedCount(&m, id, string(r.Status))
			switch r.Status {
			case a.Granted:
				m.Granted++
			case a.Queued:
				m.Queued++
			default:
				m.Terminal++
			}
		}
		m.Bytes, err = b.r.MemoryUsage(ctx, b.key, 0).Result()
		return m, err
	}
	err := b.db.QueryRowContext(ctx, "SELECT count(*),count(*) FILTER(WHERE status='granted'),count(*) FILTER(WHERE status='queued'),coalesce(sum(pg_column_size(q)),0) FROM "+b.qt+" q WHERE namespace=$1", b.namespace).Scan(&m.Records, &m.Granted, &m.Queued, &m.RowBytes)
	m.Terminal = m.Records - m.Granted - m.Queued
	if err == nil {
		rows, e := b.db.QueryContext(ctx, "SELECT id,status FROM "+b.qt+" WHERE namespace=$1", b.namespace)
		if e != nil {
			return m, e
		}
		for rows.Next() {
			var id, status string
			if e = rows.Scan(&id, &status); e != nil {
				rows.Close()
				return m, e
			}
			b.seedCount(&m, id, status)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return m, e
		}
	}
	if err != nil {
		return m, err
	}
	err = b.db.QueryRowContext(ctx, "SELECT pg_total_relation_size($1::regclass)+pg_total_relation_size($2::regclass),pg_indexes_size($1::regclass)+pg_indexes_size($2::regclass)", b.qt, b.rt).Scan(&m.Bytes, &m.IndexBytes)
	return m, err
}

func (b *backend) seedCount(m *population, id, status string) {
	switch b.seedRoles[id] {
	case "history":
		m.SeedHistory++
	case "active":
		switch a.Status(status) {
		case a.Granted:
			m.SeedActive++
		case a.Queued:
			m.SeedQueued++
		case a.Expired:
			m.SeedExpired++
		}
	}
}

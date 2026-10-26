package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/internal/driverconformance"
)

func fixture(t *testing.T) (*Driver, *goredis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = client.Close() })
	d, err := New(client, "test")
	if err != nil {
		t.Fatal(err)
	}
	return d, client, server
}

func request() airlock.Request { return requestAt(time.Now()) }

func requestAt(now time.Time) airlock.Request {
	now = now.UTC().Truncate(time.Millisecond)
	r := airlock.NewRequest(now.Add(10 * time.Second))
	r.Resource = "resource"
	r.Weight = 1
	r.WaitUntil = now.Add(10 * time.Second)
	r.Config = airlock.ResourceConfig{Admission: airlock.Config{Capacity: 1, MaxQueued: 10, MaxQueuedPerTenant: 10}, LeaseTTL: time.Second, MaxWait: 10 * time.Second, SubmitWindow: 10 * time.Second, Retention: time.Second, MaxRecords: 64}
	return r
}

func submit(r airlock.Request) airlock.Command {
	return airlock.Command{Kind: airlock.Submit, Request: r}
}

func mustExecute(t *testing.T, d *Driver, c airlock.Command) airlock.Result {
	t.Helper()
	result, err := driverconformance.Invoke(context.Background(), d, c)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestConformance(t *testing.T) {
	driverconformance.Run(t, func(t *testing.T) airlock.Driver {
		d, _, _ := fixture(t)
		if os.Getenv("AIRLOCK_STRESS") == "1" {
			// Match the conformance harness's heavy budget; otherwise the
			// adapter's inner five-second context still wins under contention.
			d.timeout = 20 * time.Second
		}
		return d
	})
}

func TestConstructor(t *testing.T) {
	d, client, _ := fixture(t)
	if d.timeout != 5*time.Second {
		t.Fatal(d.timeout)
	}

	good, err := New(client, "", WithOperationTimeout(time.Second))
	if err != nil || good.timeout != time.Second {
		t.Fatal(good, err)
	}

	disabled := goredis.NewClient(&goredis.Options{})
	defer func() { _ = disabled.Close() }()
	var nilClient *goredis.Client
	var nilCluster *goredis.ClusterClient

	for _, c := range []goredis.UniversalClient{nil, nilClient, nilCluster, disabled} {
		if _, err := New(c, ""); err == nil {
			t.Fatal("accepted invalid client")
		}
	}

	for _, namespace := range []string{strings.Repeat("x", 257), "bad\x00", string([]byte{255})} {
		if _, err := New(client, namespace); err == nil {
			t.Fatal("accepted namespace")
		}
	}

	for _, opt := range []Option{nil, WithOperationTimeout(0), WithOperationTimeout(-time.Second)} {
		if _, err := New(client, "", opt); err == nil {
			t.Fatal("accepted option")
		}
	}

	for _, o := range []*goredis.ClusterOptions{{}, {ContextTimeoutEnabled: true, ReadOnly: true}, {ContextTimeoutEnabled: true, RouteByLatency: true}, {ContextTimeoutEnabled: true, RouteRandomly: true}, {ContextTimeoutEnabled: true}} {
		c := goredis.NewClusterClient(o)
		_, err := New(c, "")
		_ = c.Close()
		if (err == nil) != (o.ContextTimeoutEnabled && !o.ReadOnly && !o.RouteByLatency && !o.RouteRandomly) {
			t.Fatal(o, err)
		}
	}
}

func TestValidationAndCorrupt(t *testing.T) {
	d, client, _ := fixture(t)
	ctx := context.Background()
	r := request()

	for _, invalid := range []*Driver{nil, {}} {
		if _, err := invalid.Submit(ctx, r); err == nil {
			t.Fatal("zero driver")
		}
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := d.Submit(cancelled, r); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	bad := r
	bad.Weight = 0
	if _, err := d.Submit(ctx, bad); !errors.Is(err, airlock.ErrInvalidWeight) {
		t.Fatal(err)
	}

	for _, value := range []string{"", "corrupt", "{}"} {
		if err := client.HSet(ctx, d.key(r.Resource), "state", value).Err(); err != nil {
			t.Fatal(err)
		}
		if result, err := d.Submit(ctx, r); !errors.Is(err, airlock.ErrCorruptState) || result != (airlock.Result{}) {
			t.Fatal(result, err)
		}
	}

	if err := client.Del(ctx, d.key(r.Resource)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.LPush(ctx, d.key(r.Resource), "x").Err(); err != nil {
		t.Fatal(err)
	}
	if result, err := d.Submit(ctx, r); err == nil || result != (airlock.Result{}) {
		t.Fatal(result, err)
	}
}

func TestPersistenceAndReload(t *testing.T) {
	d, client, server := fixture(t)
	ctx := context.Background()
	r := request()
	first := mustExecute(t, d, submit(r))
	if first.Status != airlock.Granted {
		t.Fatal(first)
	}
	if ttl := client.PTTL(ctx, d.key(r.Resource)).Val(); ttl != -1 {
		t.Fatal(ttl)
	}
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	another := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
	defer func() { _ = another.Close() }()
	d2, err := New(another, d.namespace)
	if err != nil {
		t.Fatal(err)
	}

	got := mustExecute(t, d2, submit(r))
	if got.LeaseID != first.LeaseID || got.Revision <= first.Revision {
		t.Fatal(first, got)
	}

	isolated, err := New(client, "other")
	if err != nil {
		t.Fatal(err)
	}
	if got := mustExecute(t, isolated, submit(r)); got.Status != airlock.Granted {
		t.Fatal(got)
	}
	if d.key("a") == isolated.key("a") || d.key("ab") == d.key("a") {
		t.Fatal("key collision")
	}
}

type hook struct {
	process func(context.Context, goredis.Cmder, goredis.ProcessHook) error
}

func (h hook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h hook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func (h hook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error { return h.process(ctx, cmd, next) }
}

func isCAS(cmd goredis.Cmder) bool {
	return (cmd.Name() == "eval" || cmd.Name() == "evalsha") && len(cmd.Args()) >= 9
}

func TestLostWriteResponseNeverRetries(t *testing.T) {
	for _, operation := range []airlock.Operation{airlock.Submit, airlock.Renew, airlock.Cancel, airlock.Release} {
		t.Run(string(operation), func(t *testing.T) {
			d, client, _ := fixture(t)
			r := request()
			command := submit(r)
			if operation != airlock.Submit {
				grant := mustExecute(t, d, command)
				command.Kind = operation
				command.LeaseID = grant.LeaseID
			}

			var commits atomic.Int32
			client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
				if (cmd.Name() == "eval" || cmd.Name() == "evalsha") && !cmd.NoRetry() {
					t.Error("adapter command allows network retry")
				}

				err := next(ctx, cmd)
				if isCAS(cmd) && err == nil && cmd.(*noRetryCmd).Val() == int64(1) {
					commits.Add(1)
					return io.ErrUnexpectedEOF
				}
				return err
			}})

			result, err := driverconformance.Invoke(context.Background(), d, command)
			if !errors.Is(err, io.ErrUnexpectedEOF) || result != (airlock.Result{}) || commits.Load() != 1 {
				t.Fatal(result, err, commits.Load())
			}

			raw, err := client.HGet(context.Background(), d.key(r.Resource), "state").Result()
			if err != nil {
				t.Fatal(err)
			}

			_, recovered, recoveryErr := transitionSnapshot([]byte(raw), time.Now(), airlock.Command{Kind: airlock.Observe, Request: r})
			if recoveryErr != nil {
				t.Fatal(recoveryErr)
			}

			want := airlock.Granted
			if operation == airlock.Cancel {
				want = airlock.Cancelled
			}

			if operation == airlock.Release {
				want = airlock.Released
			}

			if recovered.Status != want {
				t.Fatal(recovered)
			}
		})
	}
}

func TestScriptFailureAndMalformedResponse(t *testing.T) {
	tests := []struct {
		name     string
		cas      bool
		response any
		err      error
	}{
		{"read transport", false, nil, io.ErrUnexpectedEOF}, {"read format", false, "bad", nil},
		{"write transport", true, nil, io.ErrUnexpectedEOF}, {"write format", true, "bad", nil}, {"write code", true, int64(2), nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, client, _ := fixture(t)
			var calls int
			client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
				if isCAS(cmd) == test.cas {
					calls++
					cmd.(*noRetryCmd).SetVal(test.response)
					return test.err
				}
				return next(ctx, cmd)
			}})

			got, err := d.Submit(context.Background(), request())
			if err == nil || got != (airlock.Result{}) || calls != 1 {
				t.Fatal(got, err, calls)
			}
		})
	}
}

func TestCASConflictRetryAndDeadline(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(fmt.Sprint(permanent), func(t *testing.T) {
			d, client, _ := fixture(t)
			// Connection setup is not the CAS retry under test. Warm it before
			// applying the short deadline to the permanent-conflict case.
			warmup, cancelWarmup := context.WithTimeout(context.Background(), time.Second)
			if err := client.Ping(warmup).Err(); err != nil {
				cancelWarmup()
				t.Fatal(err)
			}

			cancelWarmup()
			if permanent {
				d.timeout = 50 * time.Millisecond
			}

			var calls int
			client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
				if isCAS(cmd) {
					calls++
					if permanent || calls == 1 {
						cmd.(*noRetryCmd).SetVal(int64(0))
						return nil
					}
				}
				return next(ctx, cmd)
			}})

			started := time.Now()
			got, err := d.Submit(context.Background(), request())
			if permanent {
				// The operation context also sets the socket deadline. Either
				// timeout can win, including during the read after a CAS miss.
				if (!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, os.ErrDeadlineExceeded)) || got != (airlock.Result{}) {
					t.Fatal(got, err)
				}
				if time.Since(started) > time.Second {
					t.Fatal("permanent conflicts escaped the operation deadline")
				}
			} else if err != nil || got.Status != airlock.Granted {
				t.Fatal(got, err)
			}

			if calls < 2 {
				t.Fatal(calls)
			}
		})
	}
}

func TestCancelledAfterCommit(t *testing.T) {
	d, client, _ := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		err := next(ctx, cmd)
		if isCAS(cmd) && err == nil {
			cancel()
		}
		return err
	}})

	got, err := d.Submit(ctx, request())
	if !errors.Is(err, context.Canceled) || got != (airlock.Result{}) {
		t.Fatal(got, err)
	}
}

func TestDecodeAndMicro(t *testing.T) {
	for _, raw := range []any{nil, []any{}, []any{int64(2), "", "1", "0"}, []any{int64(0), "x", "1", "0"}, []any{int64(0), "", "x", "0"}, []any{int64(0), "", "1", "1000000"}, []any{int64(0), "", "9007199254", "999999"}, []any{int64(1), "", "1", "0"}, []any{int64(1), "x", 1, "0"}} {
		if _, _, _, err := decodeRead(raw); err == nil {
			t.Fatal(raw)
		}
	}

	for _, stamp := range []time.Time{time.Unix(0, 0), time.Unix(1, 1), time.UnixMicro(maxExactMicro + 1)} {
		if _, err := micro(stamp); err == nil {
			t.Fatal(stamp)
		}
	}

	if n, err := micro(time.Time{}); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if n, err := micro(time.UnixMicro(maxExactMicro)); err != nil || n != maxExactMicro {
		t.Fatal(n, err)
	}
}

// Compile-time checks retain hook method compatibility with go-redis.
var _ goredis.Hook = hook{}

func TestPriorityCursorSurvivesClientReplacement(t *testing.T) {
	d, _, server := fixture(t)
	other := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
	defer func() { _ = other.Close() }()
	nextDriver, err := New(other, d.namespace)
	if err != nil {
		t.Fatal(err)
	}

	cfg := request().Config
	cfg.Admission.Priority = &airlock.PriorityPolicy{Groups: []airlock.PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 6}, {Name: "low", Turns: 1, MaxQueued: 4}}, DefaultGroup: "low", TenantGroups: map[string]string{"high": "high"}}
	makeRequest := func(tenant string) airlock.Request {
		r := request()
		r.Config = cfg
		r.Tenant = tenant
		return r
	}
	holder := makeRequest("holder")
	grant := mustExecute(t, d, submit(holder))
	high1, high2, high3, low := makeRequest("high"), makeRequest("high"), makeRequest("high"), makeRequest("low")

	for _, r := range []airlock.Request{low, high1, high2, high3} {
		if got := mustExecute(t, nextDriver, submit(r)); got.Status != airlock.Queued {
			t.Fatal(got)
		}
	}

	mustExecute(t, d, airlock.Command{Kind: airlock.Release, Request: holder, LeaseID: grant.LeaseID})

	for _, r := range []airlock.Request{high1, high2, low, high3} {
		// Reconstruct the adapter between selection and observation. Neither local
		// state nor client connection identity may reset the persisted turn cursor.
		fresh, err := New(other, d.namespace)
		if err != nil {
			t.Fatal(err)
		}

		got := mustExecute(t, fresh, airlock.Command{Kind: airlock.Observe, Request: r})
		if got.Status != airlock.Granted {
			t.Fatal(r.Tenant, got)
		}

		mustExecute(t, d, airlock.Command{Kind: airlock.Release, Request: r, LeaseID: got.LeaseID})
	}
}

func transitionSnapshot(data []byte, now time.Time, cmd airlock.Command) ([]byte, airlock.Result, error) {
	state, err := airlock.DecodeState(data)
	if err != nil {
		return nil, airlock.Result{}, err
	}

	next, result, _, err := airlock.Transition(state, now, cmd)
	if next == nil {
		return nil, result, err
	}

	encoded, codecErr := airlock.EncodeState(next)
	if codecErr != nil {
		return nil, airlock.Result{}, codecErr
	}
	return encoded, result, err
}

func TestObserveUsesOnlyRequestProjection(t *testing.T) {
	d, client, server := fixture(t)
	ctx := context.Background()
	r := request()
	server.SetTime(time.Now())
	r.Config.LeaseTTL = time.Minute
	grant, err := d.Submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}

	pending := r
	id := airlock.NewRequest(r.SubmitUntil)
	pending.ID, pending.Owner, pending.Tenant = id.ID, id.Owner, "waiting"
	queued, err := d.Submit(ctx, pending)
	if err != nil || queued.Status != airlock.Queued {
		t.Fatalf("%+v %v", queued, err)
	}

	before := client.HGetAll(ctx, d.key(r.Resource)).Val()
	var calls int
	client.AddHook(hook{process: func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		if cmd.Name() == "eval" || cmd.Name() == "evalsha" {
			calls++
			if len(cmd.Args()) != 5 {
				t.Errorf("poll performed full-state read or CAS: %v", cmd.Args()[0])
			}
		}
		return next(ctx, cmd)
	}})

	for range 10 {
		result, err := d.Observe(ctx, pending)
		if err != nil || result.Status != airlock.Queued || result.Revision != queued.Revision {
			t.Fatalf("%+v %v", result, err)
		}
	}

	if calls > 11 {
		t.Fatalf("poll commands=%d", calls)

	} // First use may load the script.
	after := client.HGetAll(ctx, d.key(r.Resource)).Val()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("read-only Observe changed stored state")
	}

	_ = grant
}

func TestObserveIncompleteIndexFallsBack(t *testing.T) {
	for _, mode := range []string{"missing", "partial-candidate", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			d, client, _ := fixture(t)
			ctx := context.Background()
			r := request()
			grant, err := d.Submit(ctx, r)
			if err != nil {
				t.Fatal(err)
			}

			key := d.key(r.Resource)

			switch mode {
			case "missing":
				if err := client.HDel(ctx, key, "r:"+r.ID).Err(); err != nil {
					t.Fatal(err)
				}
			case "partial-candidate":
				raw, err := json.Marshal(observationRecord{Token: "unpublished-candidate", Record: &airlock.DriverRecord{Request: r, Status: airlock.Granted}})
				if err != nil {
					t.Fatal(err)
				}
				if err := client.HSet(ctx, key, "r:"+r.ID, raw).Err(); err != nil {
					t.Fatal(err)
				}

			case "malformed":
				if err := client.HSet(ctx, key, "meta", "{").Err(); err != nil {
					t.Fatal(err)
				}
			}

			got, err := d.Observe(ctx, r)
			if mode == "malformed" {
				if !errors.Is(err, airlock.ErrCorruptState) || got != (airlock.Result{}) {
					t.Fatalf("%+v %v", got, err)
				}
			} else if err != nil || got.Status != airlock.Granted || got.LeaseID != grant.LeaseID {
				t.Fatalf("%+v %v", got, err)
			}
		})
	}
}

func TestObservationIndexCollectsDeletedRecords(t *testing.T) {
	d, client, server := fixture(t)
	ctx := context.Background()
	r := request()
	now := time.Now().UTC().Truncate(time.Millisecond)
	server.SetTime(now)
	grant, err := d.Submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, d.key(r.Resource), "r:orphan", "unpublished").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Release(ctx, airlock.LeaseRef{Request: r, ID: grant.LeaseID}); err != nil {
		t.Fatal(err)
	}
	if client.HExists(ctx, d.key(r.Resource), "r:orphan").Val() {
		t.Fatal("orphaned index survived next commit")
	}

	server.SetTime(r.SubmitUntil.Add(r.Config.Retention + time.Millisecond))
	_, _ = d.Observe(ctx, r)
	if client.HExists(ctx, d.key(r.Resource), "r:"+r.ID).Val() {
		t.Fatal("history GC leaked indexed request")
	}
}

func TestObserveReplyErrors(t *testing.T) {
	for _, mode := range []string{"transport", "arity", "field", "time", "record-json"} {
		t.Run(mode, func(t *testing.T) {
			d, client, _ := fixture(t)
			r := request()
			client.AddHook(hook{process: func(_ context.Context, cmd goredis.Cmder, _ goredis.ProcessHook) error {
				c := cmd.(*noRetryCmd)
				if mode == "transport" {
					return errors.New("read failed")
				}

				switch mode {
				case "arity":
					c.SetVal([]any{})
				case "field":
					c.SetVal([]any{int64(1), "", "1", "1"})
				case "time":
					c.SetVal([]any{"", "", "invalid", "1"})
				case "record-json":
					c.SetVal([]any{"{}", "{", "1800000000", "0"})
				}

				return nil
			}})

			if result, _, err := d.observe(context.Background(), r); err == nil || result != (airlock.Result{}) {
				t.Fatalf("%+v %v", result, err)
			}
		})
	}
}

func TestIndexEncodingErrors(t *testing.T) {
	invalid := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := index(&airlock.SharedState{Time: invalid}); err == nil {
		t.Fatal("invalid header time encoded")
	}

	s := &airlock.SharedState{Records: map[string]*airlock.DriverRecord{"bad": {Until: invalid, Status: airlock.Granted}}}
	if _, err := index(s); err == nil {
		t.Fatal("invalid record time encoded")
	}
}

func TestObserveCancelledAfterRead(t *testing.T) {
	d, client, _ := fixture(t)
	r := request()
	if _, err := d.Submit(context.Background(), r); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.AddHook(hook{process: func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		err := next(ctx, cmd)
		if err == nil {
			cancel()
		}
		return err
	}})

	if result, err := d.Observe(ctx, r); !errors.Is(err, context.Canceled) || result != (airlock.Result{}) {
		t.Fatalf("%+v %v", result, err)
	}
}

func TestObserveMissingAuthoritativeSnapshotFailsClosed(t *testing.T) {
	d, client, _ := fixture(t)
	r := request()
	ctx := context.Background()
	if err := client.HSet(ctx, d.key(r.Resource), "r:orphan", "unpublished").Err(); err != nil {
		t.Fatal(err)
	}
	if result, err := d.Observe(ctx, r); err == nil || result != (airlock.Result{}) {
		t.Fatalf("%+v %v", result, err)
	}
}

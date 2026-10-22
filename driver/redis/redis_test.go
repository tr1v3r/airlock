package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
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
func request() airlock.Request {
	now := time.Now().UTC().Truncate(time.Millisecond)
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
	driverconformance.Run(t, func(t *testing.T) airlock.Driver { d, _, _ := fixture(t); return d })
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
	for _, prefix := range []string{strings.Repeat("x", 257), "bad\x00", string([]byte{255})} {
		if _, err := New(client, prefix); err == nil {
			t.Fatal("accepted prefix")
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
		if err := client.Set(ctx, d.key(r.Resource), value, 0).Err(); err != nil {
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
	d2, err := New(another, d.prefix)
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
	return (cmd.Name() == "eval" || cmd.Name() == "evalsha") && len(cmd.Args()) == 9
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
			raw, err := client.Get(context.Background(), d.key(r.Resource)).Result()
			if err != nil {
				t.Fatal(err)
			}
			_, recovered, recoveryErr := airlock.Transition([]byte(raw), time.Now(), airlock.Command{Kind: airlock.Observe, Request: r})
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
			d.timeout = 50 * time.Millisecond
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
			got, err := d.Submit(context.Background(), request())
			if permanent {
				if !errors.Is(err, context.DeadlineExceeded) || got != (airlock.Result{}) {
					t.Fatal(got, err)
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
	nextDriver, err := New(other, d.prefix)
	if err != nil {
		t.Fatal(err)
	}
	cfg := request().Config
	cfg.Admission.Priority = &airlock.PriorityPolicy{Groups: []airlock.PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 6}, {Name: "low", Turns: 1, MaxQueued: 4}}, DefaultGroup: "low", TenantGroups: map[string]string{"high": "high"}}
	makeRequest := func(tenant string) airlock.Request { r := request(); r.Config = cfg; r.Tenant = tenant; return r }
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
		fresh, err := New(other, d.prefix)
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

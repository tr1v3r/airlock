package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/internal/driverconformance"
)

// Lua errors do not roll back prior writes. Assert preservation for every failed
// validation/comparison, not just the last value after a sequence of scripts.
func TestLuaCASRejectedCandidatePreservesState(t *testing.T) {
	now := time.Unix(1800000000, 123456000).UTC()
	stamp := now.UnixMicro()
	cases := []struct {
		name      string
		args      []any
		wantError bool
	}{
		{"short_arity", []any{"1", "old"}, true},
		{"long_arity", []any{"1", "old", "new", stamp, 0, "extra"}, true},
		{"invalid_existence", []any{"true", "old", "new", stamp, 0}, true},
		{"empty_candidate", []any{"1", "old", "", stamp, 0}, true},
		{"nondecimal_lower", []any{"1", "old", "new", "1e3", 0}, true},
		{"negative_upper", []any{"1", "old", "new", stamp, -1}, true},
		{"fractional_upper", []any{"1", "old", "new", stamp, "1.5"}, true},
		{"inexact_upper", []any{"1", "old", "new", stamp, "9007199254740992"}, true},
		{"empty_window", []any{"1", "old", "new", stamp, stamp}, true},
		{"expected_absent", []any{"0", "", "new", stamp, 0}, false},
		{"stale_value", []any{"1", "different", "new", stamp, 0}, false},
		{"clock_before_lower", []any{"1", "old", "new", stamp + 1, 0}, false},
		{"clock_at_exclusive_upper", []any{"1", "old", "new", stamp - 1, stamp}, false},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			d, client, server := fixture(t)
			server.SetTime(now)
			ctx := context.Background()
			key := d.key(test.name)
			if err := client.HSet(ctx, key, "state", "old").Err(); err != nil {
				t.Fatal(err)
			}

			got, err := evalTestCAS(t, d, key, test.args...)
			if (err != nil) != test.wantError || (!test.wantError && got != int64(0)) {
				t.Fatalf("CAS result=%v err=%v wantError=%t", got, err, test.wantError)
			}

			value, err := client.HGet(ctx, key, "state").Result()
			if err != nil || value != "old" {
				t.Fatalf("rejected candidate changed state: %q err=%v", value, err)
			}
			if ttl := client.PTTL(ctx, key).Val(); ttl != -1 {
				t.Fatalf("rejected candidate changed permanent lifetime: %v", ttl)
			}
		})
	}
}

func TestLuaCASTimeWindowEdges(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		name := "no_upper_bound"
		if bounded {
			name = "last_microsecond_before_upper"
		}

		t.Run(name, func(t *testing.T) {
			d, client, server := fixture(t)
			now := time.Unix(1800000000, 999999000).UTC()
			server.SetTime(now)
			var upper int64
			if bounded {
				upper = now.UnixMicro() + 1
			}

			key := d.key(name)
			ctx := context.Background()
			got, err := evalTestCAS(t, d, key, "0", "", "new", now.UnixMicro(), upper)
			if err != nil || got != int64(1) {
				t.Fatalf("inclusive lower / exclusive upper: result=%v err=%v", got, err)
			}
			if value, err := client.HGet(ctx, key, "state").Result(); err != nil || value != "new" {
				t.Fatalf("committed value=%q err=%v", value, err)
			}
		})
	}
}

func TestLuaCASWrongTypePreservesOriginal(t *testing.T) {
	d, client, _ := fixture(t)
	ctx := context.Background()
	key := d.key("wrong-type")
	if err := client.RPush(ctx, key, "original").Err(); err != nil {
		t.Fatal(err)
	}
	if got, err := evalTestCAS(t, d, key, "1", "old", "replacement", 0, 0); err == nil {
		t.Fatalf("wrong type unexpectedly accepted: %v", got)
	}

	values, err := client.LRange(ctx, key, 0, -1).Result()
	if err != nil || len(values) != 1 || values[0] != "original" {
		t.Fatalf("failed script altered list: %v err=%v", values, err)
	}
}

// Move server time between read and CAS. A Renew derived from the old live lease
// must not resurrect it once its old expiry has passed before publication.
func TestExpiryBetweenReadAndCommit(t *testing.T) {
	for _, operation := range []airlock.Operation{airlock.Renew, airlock.Cancel} {
		t.Run(string(operation), func(t *testing.T) {
			d, client, server := fixture(t)
			now := time.Now().UTC().Truncate(time.Millisecond)
			server.SetTime(now)
			// Share one time base: taking time.Now again can move deadlines
			// beyond the frozen server's MaxWait/SubmitWindow by one millisecond.
			r := requestAt(now)
			r.Config.LeaseTTL = 100 * time.Millisecond
			grant := mustExecute(t, d, submit(r))
			var shifted bool
			var conflicts int
			client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
				if isCAS(cmd) && !shifted {
					server.SetTime(now.Add(101 * time.Millisecond))
					shifted = true
				}

				err := next(ctx, cmd)
				if isCAS(cmd) && err == nil && cmd.(*noRetryCmd).Val() == int64(0) {
					conflicts++
				}
				return err
			}})

			result, err := driverconformance.Invoke(context.Background(), d, airlock.Command{Kind: operation, Request: r, LeaseID: grant.LeaseID})
			if operation == airlock.Renew {
				if !errors.Is(err, airlock.ErrLeaseLost) {
					t.Fatal(result, err)
				}
			} else if err != nil || result.Status != airlock.Expired {
				t.Fatal(result, err)
			}

			if conflicts != 1 {
				t.Fatal("time guard not exercised", conflicts)
			}

			observed := mustExecute(t, d, airlock.Command{Kind: airlock.Observe, Request: r})
			if observed.Status != airlock.Expired {
				t.Fatal(observed)
			}
		})
	}
}

func TestLuaCASGuards(t *testing.T) {
	d, client, server := fixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	server.SetTime(now)
	key := d.key("script-guards")
	stamp := strconv.FormatInt(now.UnixMicro(), 10)
	cases := []struct {
		name      string
		args      []any
		wantError bool
		want      int64
	}{
		{"arity", nil, true, 0}, {"existence", []any{"2", "", "new", stamp, 0}, true, 0},
		{"empty", []any{"0", "", "", stamp, 0}, true, 0},
		{"negative", []any{"0", "", "new", "-1", 0}, true, 0},
		{"fraction", []any{"0", "", "new", "1.5", 0}, true, 0},
		{"overflow", []any{"0", "", "new", "9007199254740992", 0}, true, 0},
		{"reversed", []any{"0", "", "new", stamp, 1}, true, 0},
		{"missing", []any{"1", "old", "new", stamp, 0}, false, 0},
		{"too_early", []any{"0", "", "new", now.Add(time.Microsecond).UnixMicro(), 0}, false, 0},
		{"expired", []any{"0", "", "new", now.Add(-time.Second).UnixMicro(), stamp}, false, 0},
		{"success", []any{"0", "", "new", stamp, 0}, false, 1},
		{"present", []any{"0", "", "newer", stamp, 0}, false, 0},
		{"mismatch", []any{"1", "other", "newer", stamp, 0}, false, 0},
		{"replace", []any{"1", "new", "newer", stamp, now.Add(time.Second).UnixMicro()}, false, 1},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			raw, err := evalTestCAS(t, d, key, test.args...)
			if test.wantError {
				if err == nil {
					t.Fatal(raw)
				}
				return
			}
			if err != nil || raw != test.want {
				t.Fatal(raw, err)
			}
		})
	}

	if got := client.HGet(ctx, key, "state").Val(); got != "newer" {
		t.Fatal(got)
	}
}

// Force a real stale-snapshot CAS, rather than making a hook return a synthetic
// conflict. The competing independent client wins all capacity after our read.
func TestCASConflictRecomputesAgainstCommittedWinner(t *testing.T) {
	d, client, server := fixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	server.SetTime(now)
	otherClient := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = otherClient.Close() })
	other, err := New(otherClient, d.namespace)
	if err != nil {
		t.Fatal(err)
	}

	candidate, winner := requestAt(now), requestAt(now)
	candidate.Config.Admission.Capacity = 3
	candidate.Weight = 3
	winner.Config, winner.Weight = candidate.Config, candidate.Weight
	var injected bool
	var conflicts, commits int
	var winning airlock.Result
	client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		if isCAS(cmd) && !injected {
			injected = true
			// Simulate a paused updater losing its lease before another client wins.
			server.FastForward(d.updateLockTTL)
			var err error
			winning, err = other.Submit(ctx, winner)
			if err != nil {
				return err
			}
		}

		err := next(ctx, cmd)
		if isCAS(cmd) && err == nil {
			switch cmd.(*noRetryCmd).Val() {
			case int64(0):
				conflicts++
			case int64(1):
				commits++
			}
		}
		return err
	}})

	got, err := d.Submit(context.Background(), candidate)
	if err != nil || got.Status != airlock.Queued || got.Weight != 3 || got.LeaseID != "" || got.Remaining != 0 {
		t.Fatalf("stale candidate must recompute as queued: %+v err=%v", got, err)
	}
	if !injected || conflicts != 1 || commits != 1 || winning.Status != airlock.Granted || got.Revision != winning.Revision+1 {
		t.Fatalf("winner=%+v candidate=%+v conflicts=%d commits=%d", winning, got, conflicts, commits)
	}

	mustExecute(t, other, airlock.Command{Kind: airlock.Release, Request: winner, LeaseID: winning.LeaseID})
	granted := mustExecute(t, other, airlock.Command{Kind: airlock.Observe, Request: candidate})
	if granted.Status != airlock.Granted || granted.Weight != 3 || granted.LeaseID == "" || granted.LeaseID == winning.LeaseID {
		t.Fatal(granted)
	}
}

// A response is not proof of non-execution merely because it is a server error.
// Only the exact NOSCRIPT error class permits the EVAL fallback.
func TestEvalDoesNotFallbackOnOtherErrors(t *testing.T) {
	for _, message := range []string{
		"NOPERM this user has no permissions",
		"OOM command not allowed when used memory > maxmemory",
		"BUSY Redis is busy running a script",
		"ERR NOSCRIPT mentioned inside an unrelated error",
	} {
		t.Run(message, func(t *testing.T) {
			d, client, _ := fixture(t)
			sentinel := errors.New(message)
			var names []string
			client.AddHook(hook{func(_ context.Context, cmd goredis.Cmder, _ goredis.ProcessHook) error {
				names = append(names, cmd.Name())
				if !cmd.NoRetry() {
					t.Error("script command permits ambiguous network retry")
				}
				return sentinel
			}})

			got, err := d.Submit(context.Background(), request())
			if !errors.Is(err, sentinel) || got != (airlock.Result{}) || len(names) != 1 || names[0] != "evalsha" {
				t.Fatalf("commands=%v result=%+v err=%v", names, got, err)
			}
		})
	}
}

// Supply valid ownership while independently exercising snapshot/window guards.
func evalTestCAS(t *testing.T, d *Driver, key string, args ...any) (any, error) {
	t.Helper()
	ctx := context.Background()
	owner := airlock.NewRequest(time.Now()).Owner
	ok, err := d.tryUpdateLock(ctx, key, owner)
	if err != nil || !ok {
		t.Fatalf("acquire test update lock: %t %v", ok, err)
	}
	defer d.releaseUpdateLock(ctx, key, owner)
	return d.evalKeys(ctx, casScript, []string{key, updateLockKey(key)}, append(args, owner)...)
}

func TestUpdateLockOwnershipAndExpiry(t *testing.T) {
	d, client, server := fixture(t)
	configured, err := New(client, d.namespace, WithUpdateLockTTL(time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := d.key("owner-check")
	lock := updateLockKey(key)
	if ok, err := configured.tryUpdateLock(ctx, key, "old"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ttl := client.PTTL(ctx, lock).Val(); ttl != time.Millisecond {
		t.Fatalf("TTL must round upward: %v", ttl)
	}
	for _, owner := range []string{"old", "other"} {
		if ok, err := d.tryUpdateLock(ctx, key, owner); err != nil || ok {
			t.Fatalf("existing lock reacquired by %q: %t %v", owner, ok, err)
		}
	}
	if ttl := client.PTTL(ctx, lock).Val(); ttl != time.Millisecond {
		t.Fatal("contention refreshed the lock", ttl)
	}

	server.FastForward(time.Millisecond)
	// Expiry without a successor must reject a stale owner too: no state has
	// changed, so the snapshot comparison alone would incorrectly accept it.
	for _, successor := range []bool{false, true} {
		if successor {
			if ok, err := d.tryUpdateLock(ctx, key, "successor"); err != nil || !ok {
				t.Fatal(ok, err)
			}
		}
		raw, err := d.evalKeys(ctx, casScript, []string{key, lock}, "0", "", "stale", 0, 0, "old")
		if err != nil || raw != int64(0) || client.Exists(ctx, key).Val() != 0 {
			t.Fatalf("expired owner published: %v %v", raw, err)
		}
		d.releaseUpdateLock(ctx, key, "old")
	}
	if owner := client.Get(ctx, lock).Val(); owner != "successor" {
		t.Fatal("stale cleanup removed successor", owner)
	}
	d.releaseUpdateLock(ctx, key, "successor")
	if client.Exists(ctx, lock).Val() != 0 {
		t.Fatal("owner cleanup left update lock")
	}
}

func TestBusyUpdateLockAvoidsFullState(t *testing.T) {
	for _, kind := range []airlock.Operation{airlock.Submit, airlock.Cancel, airlock.Renew, airlock.Release, airlock.Observe} {
		t.Run(string(kind), func(t *testing.T) {
			d, client, server := fixture(t)
			now := time.Now().UTC().Truncate(time.Millisecond)
			server.SetTime(now)
			r := requestAt(now)
			grant := mustExecute(t, d, submit(r))
			if kind == airlock.Observe {
				server.SetTime(now.Add(1100 * time.Millisecond))
			}
			key := d.key(r.Resource)
			before := client.HGetAll(context.Background(), key).Val()
			if ok, err := d.tryUpdateLock(context.Background(), key, "busy"); err != nil || !ok {
				t.Fatal(ok, err)
			}
			var tries, reads, commits int
			client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
				if isScript(cmd, acquireUpdateLockScript) {
					tries++
				}
				if isScript(cmd, readScript) {
					reads++
				}
				if isCAS(cmd) {
					commits++
				}
				return next(ctx, cmd)
			}})
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			result, err := driverconformance.Invoke(ctx, d, airlock.Command{Kind: kind, Request: r, LeaseID: grant.LeaseID})
			if err == nil || result != (airlock.Result{}) || tries == 0 || reads != 0 || commits != 0 {
				t.Fatalf("result=%+v err=%v tries=%d reads=%d commits=%d", result, err, tries, reads, commits)
			}
			if after := client.HGetAll(context.Background(), key).Val(); !reflect.DeepEqual(before, after) {
				t.Fatal("contender changed resource state")
			}
		})
	}
}

func TestObserveRechecksAfterAcquiringUpdateLock(t *testing.T) {
	d, client, server := fixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	server.SetTime(now)
	holder, pending := requestAt(now), requestAt(now)
	mustExecute(t, d, submit(holder))
	mustExecute(t, d, submit(pending))
	server.SetTime(now.Add(1100 * time.Millisecond))
	otherClient := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = otherClient.Close() })
	other, err := New(otherClient, d.namespace)
	if err != nil {
		t.Fatal(err)
	}
	var repaired airlock.Result
	var reads, acquisitions int
	client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		if isScript(cmd, acquireUpdateLockScript) && acquisitions == 0 {
			acquisitions++
			var err error
			repaired, err = other.Observe(ctx, pending)
			if err != nil {
				return err
			}
		}
		if isScript(cmd, readScript) || isCAS(cmd) {
			reads++
		}
		return next(ctx, cmd)
	}})
	got, err := d.Observe(context.Background(), pending)
	if err != nil || got.Status != airlock.Granted || got.Revision != repaired.Revision || got.LeaseID != repaired.LeaseID || reads != 0 || acquisitions != 1 {
		t.Fatalf("got=%+v winner=%+v err=%v full-state calls=%d acquisitions=%d", got, repaired, err, reads, acquisitions)
	}
	if client.Exists(context.Background(), updateLockKey(d.key(pending.Resource))).Val() != 0 {
		t.Fatal("read-only recheck leaked lock")
	}
}

func TestUpdateLockIsolatedByNamespaceAndResource(t *testing.T) {
	d, client, _ := fixture(t)
	ctx := context.Background()
	r := request()
	if ok, err := d.tryUpdateLock(ctx, d.key(r.Resource), "busy"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	other, err := New(client, "independent")
	if err != nil {
		t.Fatal(err)
	}
	mustExecute(t, other, submit(r))
	r.Resource = "another-resource"
	mustExecute(t, d, submit(r))
	key := d.key(r.Resource)
	if strings.Split(key, "}")[0] != strings.Split(updateLockKey(key), "}")[0] || key == updateLockKey(key) {
		t.Fatal("state and lock do not share a distinct-key Cluster hash tag")
	}
}

func TestUpdateLockCleanupAfterCancellation(t *testing.T) {
	d, client, _ := fixture(t)
	r := request()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		err := next(ctx, cmd)
		if isScript(cmd, readScript) && err == nil {
			cancel()
		}
		return err
	}})
	result, err := d.Submit(ctx, r)
	if !errors.Is(err, context.Canceled) || result != (airlock.Result{}) {
		t.Fatal(result, err)
	}
	key := d.key(r.Resource)
	if client.Exists(context.Background(), key, updateLockKey(key)).Val() != 0 {
		t.Fatal("cancelled update left state or ownership")
	}
}

func TestLostUpdateLockResponseDoesNotAuthorizeOrReplay(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(fmt.Sprint(release), func(t *testing.T) {
			d, client, server := fixture(t)
			r := request()
			var successes int
			client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
				err := next(ctx, cmd)
				script := acquireUpdateLockScript
				if release {
					script = releaseUpdateLockScript
				}
				if isScript(cmd, script) && err == nil && cmd.(*noRetryCmd).Val() == int64(1) {
					successes++
					if !cmd.NoRetry() {
						t.Error("lock command allowed replay")
					}
					return io.ErrUnexpectedEOF
				}
				return err
			}})
			got, err := d.Submit(context.Background(), r)
			if successes != 1 {
				t.Fatal("ambiguous mutation replayed", successes)
			}
			if release {
				// A lost cleanup reply cannot undo a confirmed state commit.
				if err != nil || got.Status != airlock.Granted {
					t.Fatal(got, err)
				}
			} else {
				if !errors.Is(err, io.ErrUnexpectedEOF) || got != (airlock.Result{}) || client.Exists(context.Background(), d.key(r.Resource)).Val() != 0 {
					t.Fatal(got, err)
				}
				server.FastForward(d.updateLockTTL)
				if client.Exists(context.Background(), updateLockKey(d.key(r.Resource))).Val() != 0 {
					t.Fatal("abandoned acquire did not expire")
				}
			}
		})
	}
}

func TestConcurrentObserveMaintainsOnce(t *testing.T) {
	d, firstClient, server := fixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	server.SetTime(now)
	holder, pending := requestAt(now), requestAt(now)
	mustExecute(t, d, submit(holder))
	queued := mustExecute(t, d, submit(pending))
	server.SetTime(now.Add(1100 * time.Millisecond))
	secondClient := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = secondClient.Close() })
	other, err := New(secondClient, d.namespace)
	if err != nil {
		t.Fatal(err)
	}
	d.updateLockTTL, other.updateLockTTL = 5*time.Second, 5*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	entered, proceed := make(chan struct{}), make(chan struct{})
	const contenders = 10
	misses := make(chan struct{}, contenders)
	var reads, commits atomic.Int32
	inspect := hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		err := next(ctx, cmd)
		if err != nil {
			return err
		}
		if isScript(cmd, acquireUpdateLockScript) && cmd.(*noRetryCmd).Val() == int64(0) {
			select {
			case misses <- struct{}{}:
			default:
			}
		}
		if isScript(cmd, readScript) && reads.Add(1) == 1 {
			close(entered)
			select {
			case <-proceed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if isCAS(cmd) && cmd.(*noRetryCmd).Val() == int64(1) {
			commits.Add(1)
		}
		return nil
	}}
	firstClient.AddHook(inspect)
	secondClient.AddHook(inspect)
	type outcome struct {
		result airlock.Result
		err    error
	}
	done := make(chan outcome, contenders+1)
	observe := func(driver *Driver) {
		result, err := driver.Observe(ctx, pending)
		done <- outcome{result, err}
	}
	go observe(d)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first observer never read the snapshot")
	}
	for range contenders {
		go observe(other)
	}
	for range contenders {
		select {
		case <-misses:
		case <-ctx.Done():
			t.Fatal("contenders did not reach the update lock")
		}
	}
	close(proceed)
	for range contenders + 1 {
		got := <-done
		if got.err != nil || got.result.Status != airlock.Granted || got.result.Revision != queued.Revision+1 {
			t.Fatalf("observer result=%+v err=%v", got.result, got.err)
		}
	}
	if reads.Load() != 1 || commits.Load() != 1 {
		t.Fatalf("duplicate maintenance: reads=%d commits=%d", reads.Load(), commits.Load())
	}
}

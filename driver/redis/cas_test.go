package redis

import (
	"context"
	"errors"
	"strconv"
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
			if err := client.Set(ctx, key, "old", 0).Err(); err != nil {
				t.Fatal(err)
			}
			got, err := d.eval(ctx, casScript, key, test.args...)
			if (err != nil) != test.wantError || (!test.wantError && got != int64(0)) {
				t.Fatalf("CAS result=%v err=%v wantError=%t", got, err, test.wantError)
			}
			value, err := client.Get(ctx, key).Result()
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
			got, err := d.eval(ctx, casScript, key, "0", "", "new", now.UnixMicro(), upper)
			if err != nil || got != int64(1) {
				t.Fatalf("inclusive lower / exclusive upper: result=%v err=%v", got, err)
			}
			if value, err := client.Get(ctx, key).Result(); err != nil || value != "new" {
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
	if got, err := d.eval(ctx, casScript, key, "1", "old", "replacement", 0, 0); err == nil {
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
			raw, err := d.eval(ctx, casScript, key, test.args...)
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
	if got := client.Get(ctx, key).Val(); got != "newer" {
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

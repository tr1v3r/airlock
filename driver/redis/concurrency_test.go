package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/tr1v3r/airlock"
)

// Force a real stale-snapshot CAS, rather than making a hook return a synthetic
// conflict. The competing independent client wins all capacity after our read.
func TestCASConflictRecomputesAgainstCommittedWinner(t *testing.T) {
	d, client, server := fixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	server.SetTime(now)
	otherClient := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = otherClient.Close() })
	other, err := New(otherClient, d.prefix)
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

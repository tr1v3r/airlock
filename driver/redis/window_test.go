package redis

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/tr1v3r/airlock"
)

// Move server time between read and CAS. A Renew derived from the old live lease
// must not resurrect it once its old expiry has passed before publication.
func TestExpiryBetweenReadAndCommit(t *testing.T) {
	for _, operation := range []airlock.Operation{airlock.Renew, airlock.Cancel} {
		t.Run(string(operation), func(t *testing.T) {
			d, client, server := fixture(t)
			now := time.Now().UTC().Truncate(time.Millisecond)
			server.SetTime(now)
			r := request()
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
			result, err := d.Execute(context.Background(), airlock.Command{Kind: operation, Request: r, LeaseID: grant.LeaseID})
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

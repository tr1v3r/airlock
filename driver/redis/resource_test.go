package redis

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/airlock"
)

func TestInspectResourceFaults(t *testing.T) {
	for _, mode := range []string{"query", "shape", "fields", "clock", "missing", "json", "header", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			d, client, _ := fixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
				if !isScript(cmd, inspectScript) {
					return next(ctx, cmd)
				}
				if mode == "query" {
					return io.ErrUnexpectedEOF
				}
				var raw any = []any{"", int64(1), "1000", "0"}
				switch mode {
				case "shape":
					raw = "bad"
				case "fields":
					raw = []any{1, "x", "1000", "0"}
				case "clock":
					raw = []any{"", int64(1), "bad", "0"}
				case "missing":
					raw = []any{"", int64(0), "1000", "0"}
				case "json":
					raw = []any{"bad", int64(1), "1000", "0"}
				case "header":
					raw = []any{`{"Token":"x"}`, int64(1), "1000", "0"}
				case "cancel":
					raw = []any{`{"Token":"x"}`, int64(1), "1000", "0"}
					cancel()
				}
				cmd.(*noRetryCmd).SetVal(raw)
				return nil
			}})
			result, err := d.InspectResource(ctx, "r")
			if err == nil || result != (airlock.ResourceStatus{}) {
				t.Fatal(result, err)
			}
		})
	}
	d, _, _ := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.InspectResource(ctx, "r"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := d.InspectResource(context.Background(), ""); !errors.Is(err, airlock.ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := (*Driver)(nil).InspectResource(context.Background(), "r"); err == nil {
		t.Fatal("nil accepted")
	}
}

func TestResizeUnknownCommitAndOwnerLoss(t *testing.T) {
	for _, mode := range []string{"lost-response", "lost-owner", "expired-window"} {
		t.Run(mode, func(t *testing.T) {
			d, client, server := fixture(t)
			r := request()
			mustExecute(t, d, submit(r))
			var attempts atomic.Int32
			client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
				if !isCommit(cmd) {
					return next(ctx, cmd)
				}
				n := attempts.Add(1)
				if n == 1 && mode == "lost-owner" {
					server.FastForward(2 * time.Second)
				}
				if n == 1 && mode == "expired-window" {
					server.SetTime(time.Now().Add(2 * time.Second))
				}
				err := next(ctx, cmd)
				if mode == "lost-response" && err == nil && cmd.(*noRetryCmd).Val() == int64(1) {
					return io.ErrUnexpectedEOF
				}
				return err
			}})
			result, err := d.Resize(context.Background(), airlock.ResizeRequest{Resource: r.Resource, Capacity: 2, ExpectedVersion: 1})
			if mode == "lost-response" {
				if !errors.Is(err, io.ErrUnexpectedEOF) || result != (airlock.ResourceStatus{}) || attempts.Load() != 1 {
					t.Fatal(result, err, attempts.Load())
				}
				status, e := d.InspectResource(context.Background(), r.Resource)
				if e != nil || status.CapacityVersion != 2 {
					t.Fatal(status, e)
				}
			} else if err != nil || result.Capacity != 2 || attempts.Load() < 2 {
				t.Fatal(result, err, attempts.Load())
			}
		})
	}
}

func TestInspectIsReadOnlyDuringOverdueMaintenance(t *testing.T) {
	d, client, server := fixture(t)
	r := request()
	mustExecute(t, d, submit(r))
	ctx := context.Background()
	key := d.key(r.Resource)
	before, err := client.HGet(ctx, key, "state").Result()
	if err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.Now().Add(2 * time.Second))
	if err := client.Set(ctx, updateLockKey(key), "held-by-test", 0).Err(); err != nil {
		t.Fatal(err)
	}
	status, err := d.InspectResource(ctx, r.Resource)
	if err != nil || status.Used != 1 {
		t.Fatal(status, err)
	}
	after, err := client.HGet(ctx, key, "state").Result()
	if err != nil || after != before {
		t.Fatal("Inspect maintained state", err)
	}
	if got, err := client.Get(ctx, updateLockKey(key)).Result(); err != nil || got != "held-by-test" {
		t.Fatal("Inspect changed lock", got, err)
	}
	if err := client.Del(ctx, updateLockKey(key)).Err(); err != nil {
		t.Fatal(err)
	}
	maintained, err := d.Resize(ctx, airlock.ResizeRequest{Resource: r.Resource, Capacity: 1, ExpectedVersion: 1})
	if err != nil || maintained.Used != 0 || maintained.CapacityVersion != 1 || maintained.Revision <= status.Revision {
		t.Fatal(maintained, err)
	}
}

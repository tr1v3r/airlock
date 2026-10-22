package redis

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/tr1v3r/airlock"
)

func distributed(t *testing.T, d *Driver, resource string) *airlock.Shared {
	t.Helper()
	cfg := request().Config
	cfg.LeaseTTL = 2 * time.Second
	client, err := airlock.NewShared(d, airlock.SharedOptions{Resource: resource, Config: cfg, OperationTimeout: time.Second, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func TestDistributedLeaseLifecycle(t *testing.T) {
	d, _, _ := fixture(t)
	client := distributed(t, d, "lifecycle")
	ctx, cancel := context.WithCancel(context.Background())
	lease, err := client.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if !lease.Valid() || lease.Weight() != 1 {
		t.Fatal("acquisition cancellation revoked delivered lease")
	}
	wait, cancelWait := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelWait()
	if blocked, err := client.Acquire(wait); (!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, os.ErrDeadlineExceeded)) || blocked != nil {
		t.Fatal(blocked, err)
	}
	if err := lease.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lease.Valid() {
		t.Fatal("released lease valid")
	}
	select {
	case <-lease.Done():
	default:
		t.Fatal("release did not close Done")
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	next, err := client.AcquireFor(context.Background(), "next")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-next.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("lease did not expire")
	}
	if next.Valid() {
		t.Fatal("expired lease valid")
	}
	if err := next.Renew(context.Background()); !errors.Is(err, airlock.ErrLeaseLost) {
		t.Fatal(err)
	}
	replacement, err := client.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestDistributedUnknownRenewInvalidates(t *testing.T) {
	d, client, _ := fixture(t)
	distributed := distributed(t, d, "unknown-renew")
	lease, err := distributed.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	dropped := false
	client.AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		err := next(ctx, cmd)
		if isCAS(cmd) && err == nil && !dropped && cmd.(*noRetryCmd).Val() == int64(1) {
			dropped = true
			return io.ErrUnexpectedEOF
		}
		return err
	}})
	if err := lease.Renew(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if lease.Valid() {
		t.Fatal("unknown renewal retained authority")
	}
	select {
	case <-lease.Done():
	default:
		t.Fatal("unknown renewal did not close Done")
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

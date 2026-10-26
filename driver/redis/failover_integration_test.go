package redis_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/driver/redis"
)

type failoverFixture struct {
	Request  airlock.Request
	LeaseID  string
	Deadline time.Time
}

// This deliberately proves a limitation: a stale asynchronous replica can
// grant capacity already acknowledged by the old, now fenced primary.
func TestAsynchronousFailoverLosesAcknowledgedGrant(t *testing.T) {
	mode := os.Getenv("AIRLOCK_REDIS_FAILOVER_MODE")
	if mode == "" {
		t.Skip("run scripts/redis-failover.sh")
	}

	path, addr := os.Getenv("AIRLOCK_REDIS_FAILOVER_FIXTURE"), os.Getenv("AIRLOCK_REDIS_ADDR")
	if path == "" || addr == "" {
		t.Fatal("missing isolated failover fixture or address")
	}

	c := goredis.NewClient(&goredis.Options{Addr: addr, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = c.Close() })
	d, err := redis.New(c, "async-failover")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch mode {
	case "seed":
		r := airlock.NewRequest(time.Now().Add(time.Minute))
		r.Config = airlock.ResourceConfig{Admission: airlock.Config{Capacity: 1, MaxQueued: 4, MaxQueuedPerTenant: 4}, LeaseTTL: 2 * time.Minute, MaxWait: time.Minute, SubmitWindow: time.Minute, Retention: time.Minute, MaxRecords: 20}
		r.Resource, r.Weight, r.WaitUntil = "resource", 1, r.SubmitUntil
		start := time.Now()
		got, err := d.Submit(ctx, r)
		if err != nil || got.Status != airlock.Granted {
			t.Fatalf("seed: %+v %v", got, err)
		}

		fixture := failoverFixture{Request: r, LeaseID: got.LeaseID, Deadline: start.Add(got.Remaining)}
		data, err := json.Marshal(fixture)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}

	case "verify":
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}

		var fixture failoverFixture
		if err := json.Unmarshal(data, &fixture); err != nil {
			t.Fatal(err)
		}
		if !time.Now().Before(fixture.Deadline) {
			t.Fatal("fault took longer than old lease; overlap was not demonstrated")
		}

		got, err := d.Observe(ctx, fixture.Request)
		if !errors.Is(err, airlock.ErrNotFound) || got.Status != "" || got.LeaseID != "" || got.Weight != 0 {
			t.Fatalf("expected lost grant: %+v %v", got, err)
		}

		fresh := airlock.NewRequest(time.Now().Add(30 * time.Second))
		fresh.Config, fresh.Resource, fresh.Weight, fresh.WaitUntil = fixture.Request.Config, fixture.Request.Resource, 1, fresh.SubmitUntil
		got, err = d.Submit(ctx, fresh)
		if err != nil || got.Status != airlock.Granted || got.LeaseID == fixture.LeaseID {
			t.Fatalf("new grant: %+v %v", got, err)
		}
		if !time.Now().Before(fixture.Deadline) {
			t.Fatal("new grant did not overlap old acknowledged lease")
		}

		t.Log("confirmed: stale promotion grants full capacity while original acknowledged lease has time remaining")

	default:
		t.Fatalf("unknown mode %q", mode)
	}
}

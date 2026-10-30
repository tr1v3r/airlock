package redis

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/internal/driverconformance"
)

func integrationClient(t *testing.T, cluster bool) goredis.UniversalClient {
	t.Helper()
	var client goredis.UniversalClient
	if cluster {
		addresses := os.Getenv("AIRLOCK_REDIS_CLUSTER_ADDRS")
		if addresses == "" {
			t.Skip("set AIRLOCK_REDIS_CLUSTER_ADDRS to run real Redis Cluster tests")
		}

		mapping := map[string]string{}
		if raw := os.Getenv("AIRLOCK_REDIS_CLUSTER_MAPPING"); raw != "" {
			for _, entry := range strings.Split(raw, ",") {
				pair := strings.SplitN(entry, "=", 2)
				if len(pair) != 2 || pair[0] == "" || pair[1] == "" {
					t.Fatal("invalid AIRLOCK_REDIS_CLUSTER_MAPPING")
				}

				mapping[pair[0]] = pair[1]
			}
		}

		options := &goredis.ClusterOptions{Addrs: strings.Split(addresses, ","), ContextTimeoutEnabled: true}
		options.Dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
			if mapped, ok := mapping[address]; ok {
				address = mapped
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
		client = goredis.NewClusterClient(options)
	} else {
		address := os.Getenv("AIRLOCK_REDIS_ADDR")
		if address == "" {
			t.Skip("set AIRLOCK_REDIS_ADDR to run real Redis tests")
		}

		client = goredis.NewClient(&goredis.Options{Addr: address, ContextTimeoutEnabled: true})
	}

	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestIntegrationStandalone(t *testing.T) { integrationSuite(t, false) }

func TestIntegrationCluster(t *testing.T) { integrationSuite(t, true) }

func integrationSuite(t *testing.T, cluster bool) {
	client := integrationClient(t, cluster)
	namespace := "airlock-test-" + airlock.NewRequest(time.Now()).Owner
	factory := func(t *testing.T) airlock.Driver {
		t.Helper()
		d, err := New(client, namespace)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	driverconformance.Run(t, factory)
	t.Run("partitioned_history_migration", func(t *testing.T) { integrationPartitionHistory(t, client, namespace+"-history") })
	t.Run("shared_clients_and_permanent_state", func(t *testing.T) {
		other := integrationClient(t, cluster)
		first, err := New(client, namespace)
		if err != nil {
			t.Fatal(err)
		}

		second, err := New(other, namespace)
		if err != nil {
			t.Fatal(err)
		}

		r := request()
		r.Resource = airlock.NewRequest(time.Now()).Owner
		a := mustExecute(t, first, submit(r))
		b := mustExecute(t, second, submit(r))
		if a.LeaseID != b.LeaseID || b.Revision <= a.Revision {
			t.Fatal(a, b)
		}
		if ttl := client.PTTL(context.Background(), first.key(r.Resource)).Val(); ttl != -1 {
			t.Fatal(ttl)
		}

		mustExecute(t, second, airlock.Command{Kind: airlock.Release, Request: r, LeaseID: a.LeaseID})
	})

	t.Run("update_lock_blocks_writes_not_reads", func(t *testing.T) {
		other := integrationClient(t, cluster)
		first := factory(t).(*Driver)
		second, err := New(other, namespace)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		r := request()
		r.Resource = airlock.NewRequest(time.Now()).Owner
		grant := mustExecute(t, first, submit(r))
		key := first.key(r.Resource)
		if ok, err := first.tryUpdateLock(ctx, key, "busy-updater"); err != nil || !ok {
			t.Fatal(ok, err)
		}
		defer first.releaseUpdateLock(ctx, key, "busy-updater")
		observed := mustExecute(t, second, airlock.Command{Kind: airlock.Observe, Request: r})
		if observed.LeaseID != grant.LeaseID || observed.Revision != grant.Revision {
			t.Fatal(observed)
		}
		bounded, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		if result, err := second.Renew(bounded, airlock.LeaseRef{Request: r, ID: grant.LeaseID}); err == nil || result != (airlock.Result{}) {
			t.Fatal("write bypassed update lock", result, err)
		}
		independent := request()
		independent.Resource = airlock.NewRequest(time.Now()).Owner
		mustExecute(t, second, submit(independent))
		first.releaseUpdateLock(ctx, key, "busy-updater")
		mustExecute(t, second, airlock.Command{Kind: airlock.Release, Request: r, LeaseID: grant.LeaseID})
		if client.Exists(ctx, updateLockKey(key)).Val() != 0 {
			t.Fatal("completed command left lock")
		}
	})

	t.Run("expired_owner_cannot_commit_or_unlock_successor", func(t *testing.T) {
		d := factory(t).(*Driver)
		ctx := context.Background()
		key := d.key(airlock.NewRequest(time.Now()).Owner)
		d.updateLockTTL = time.Millisecond
		if ok, err := d.tryUpdateLock(ctx, key, "expired-owner"); err != nil || !ok {
			t.Fatal(ok, err)
		}
		// Redis must expire this lease itself; no administrative DEL is used.
		deadline := time.Now().Add(time.Second)
		for client.Exists(ctx, updateLockKey(key)).Val() != 0 {
			if time.Now().After(deadline) {
				t.Fatal("update lock did not expire")
			}
			time.Sleep(time.Millisecond)
		}
		d.updateLockTTL = time.Second
		if ok, err := d.tryUpdateLock(ctx, key, "successor"); err != nil || !ok {
			t.Fatal(ok, err)
		}
		defer d.releaseUpdateLock(ctx, key, "successor")
		raw, err := d.evalKeys(ctx, commitScript, []string{key, updateLockKey(key)}, "expired-owner", "stale", 0, 0, `{"Meta":"","Records":{}}`)
		if err != nil || raw != int64(0) || client.Exists(ctx, key).Val() != 0 {
			t.Fatal("expired owner published", raw, err)
		}
		d.releaseUpdateLock(ctx, key, "expired-owner")
		if owner := client.Get(ctx, updateLockKey(key)).Val(); owner != "successor" {
			t.Fatal("expired owner removed successor", owner)
		}
	})

	t.Run("script_cache_flush", func(t *testing.T) {
		// Use only an isolated test server: flushing script cache affects all users.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if c, ok := client.(*goredis.ClusterClient); ok {
			if err := c.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error { return c.ScriptFlush(ctx).Err() }); err != nil {
				t.Fatal(err)
			}
		} else if err := client.ScriptFlush(ctx).Err(); err != nil {
			t.Fatal(err)
		}

		d := factory(t).(*Driver)
		r := request()
		r.Resource = airlock.NewRequest(time.Now()).Owner
		if got := mustExecute(t, d, submit(r)); got.Status != airlock.Granted {
			t.Fatal(got)
		}
	})
}

func BenchmarkIntegrationObserveQueued(b *testing.B) { benchmarkIntegrationObservation(b, false) }

func BenchmarkIntegrationRenewWithWaiters(b *testing.B) { benchmarkIntegrationObservation(b, true) }

func benchmarkIntegrationObservation(b *testing.B, renew bool) {
	address := os.Getenv("AIRLOCK_REDIS_ADDR")
	if address == "" {
		b.Skip("set AIRLOCK_REDIS_ADDR")
	}

	for _, count := range []int{1, 100, 1000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			client := goredis.NewClient(&goredis.Options{Addr: address, ContextTimeoutEnabled: true})
			defer func() { _ = client.Close() }()
			d, err := New(client, "observe-bench-"+airlock.NewRequest(time.Now()).Owner)
			if err != nil {
				b.Fatal(err)
			}

			r := request()
			r.Config.LeaseTTL, r.Config.MaxWait, r.Config.SubmitWindow = time.Hour, time.Hour, time.Hour
			r.Config.Admission.MaxQueued, r.Config.Admission.MaxQueuedPerTenant, r.Config.MaxRecords = 1000, 1000, 1001
			until := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
			id := airlock.NewRequest(until)
			r.ID, r.Owner, r.SubmitUntil, r.WaitUntil = id.ID, id.Owner, until, until
			ctx := context.Background()
			defer func() { _ = client.Del(ctx, d.key(r.Resource)).Err() }()
			held, err := d.Submit(ctx, r)
			if err != nil {
				b.Fatal(err)
			}

			var pending airlock.Request

			for range count {
				pending = r
				id := airlock.NewRequest(until)
				pending.ID, pending.Owner = id.ID, id.Owner
				if _, err := d.Submit(ctx, pending); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				var result airlock.Result
				var err error
				want := airlock.Queued
				if renew {
					result, err = d.Renew(ctx, airlock.LeaseRef{Request: r, ID: held.LeaseID})
					want = airlock.Granted
				} else {
					result, err = d.Observe(ctx, pending)
				}

				if err != nil || result.Status != want {
					b.Fatalf("%+v %v", result, err)
				}
			}

			b.StopTimer()
		})
	}
}

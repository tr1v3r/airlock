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
func TestIntegrationCluster(t *testing.T)    { integrationSuite(t, true) }
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

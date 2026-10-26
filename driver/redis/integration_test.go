package redis

import (
	"context"
	"net"
	"os"
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
	prefix := "airlock-test-" + airlock.NewRequest(time.Now()).Owner
	factory := func(t *testing.T) airlock.Driver {
		t.Helper()
		d, err := New(client, prefix)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	driverconformance.Run(t, factory)
	t.Run("shared_clients_and_permanent_state", func(t *testing.T) {
		other := integrationClient(t, cluster)
		first, err := New(client, prefix)
		if err != nil {
			t.Fatal(err)
		}
		second, err := New(other, prefix)
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

package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/driver/postgres"
)

type failoverFixture struct {
	Holder, Pending airlock.Request
	LeaseID         string
}

// TestSynchronousFailover is driven in two separate processes by
// scripts/postgres-failover.sh, around fencing the primary and promoting its
// synchronous remote_apply standby. It never manages production infrastructure.
func TestSynchronousFailover(t *testing.T) {
	mode := os.Getenv("AIRLOCK_PG_FAILOVER_MODE")
	if mode == "" {
		t.Skip("run scripts/postgres-failover.sh")
	}
	path := os.Getenv("AIRLOCK_PG_FAILOVER_FIXTURE")
	dsn := os.Getenv("AIRLOCK_POSTGRES_DSN")
	if path == "" || dsn == "" {
		t.Fatal("missing isolated failover fixture or DSN")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	driver, err := postgres.New(db, "synchronous-failover")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	call := func(kind airlock.Operation, r airlock.Request, lease string, want airlock.Status) airlock.Result {
		t.Helper()
		result, e := driver.Execute(ctx, airlock.Command{Kind: kind, Request: r, LeaseID: lease})
		if e != nil || result.Status != want {
			t.Fatalf("%s: %+v %v want %s", kind, result, e, want)
		}
		return result
	}
	switch mode {
	case "seed":
		if err := driver.EnsureSchema(ctx); err != nil {
			t.Fatal(err)
		}
		cfg := airlock.ResourceConfig{Admission: airlock.Config{Capacity: 3, MaxQueued: 10, MaxQueuedPerTenant: 10, Priority: &airlock.PriorityPolicy{Groups: []airlock.PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 5}, {Name: "low", Turns: 1, MaxQueued: 5}}, DefaultGroup: "low", TenantGroups: map[string]string{"online": "high"}}}, LeaseTTL: time.Minute, MaxWait: time.Minute, SubmitWindow: time.Minute, Retention: time.Minute, MaxRecords: 100}
		req := func(tenant string, n int) airlock.Request {
			now := time.Now()
			r := airlock.NewRequest(now.Add(50 * time.Second))
			r.Config = cfg
			r.Resource = "capacity"
			r.Tenant = tenant
			r.Weight = n
			r.WaitUntil = now.Add(50 * time.Second).UTC().Truncate(time.Millisecond)
			return r
		}
		fixture := failoverFixture{Holder: req("holder", 2), Pending: req("batch", 3)}
		fixture.LeaseID = call(airlock.Submit, fixture.Holder, "", airlock.Granted).LeaseID
		call(airlock.Submit, fixture.Pending, "", airlock.Queued)
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
		var recovery bool
		if err := db.QueryRowContext(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery); err != nil || recovery {
			t.Fatalf("standby not promoted: %t %v", recovery, err)
		}
		held := call(airlock.Observe, fixture.Holder, "", airlock.Granted)
		if held.LeaseID != fixture.LeaseID {
			t.Fatal("failover replaced a committed lease")
		}
		call(airlock.Observe, fixture.Pending, "", airlock.Queued)
		small := airlock.NewRequest(time.Now().Add(30 * time.Second))
		small.Config = fixture.Holder.Config
		small.Resource = fixture.Holder.Resource
		small.Tenant = "online"
		small.Weight = 1
		small.WaitUntil = small.SubmitUntil
		call(airlock.Submit, small, "", airlock.Queued)
		call(airlock.Release, fixture.Holder, held.LeaseID, airlock.Released)
		call(airlock.Observe, small, "", airlock.Queued)
		large := call(airlock.Observe, fixture.Pending, "", airlock.Granted)
		call(airlock.Release, fixture.Pending, large.LeaseID, airlock.Released)
		tiny := call(airlock.Observe, small, "", airlock.Granted)
		call(airlock.Submit, fixture.Holder, "", airlock.Released)
		wrong := small
		wrong.Owner = airlock.NewRequest(time.Now()).Owner
		if _, err := driver.Execute(ctx, airlock.Command{Kind: airlock.Release, Request: wrong, LeaseID: tiny.LeaseID}); !errors.Is(err, airlock.ErrRequestConflict) {
			t.Fatalf("wrong owner: %v", err)
		}
		call(airlock.Release, small, tiny.LeaseID, airlock.Released)
	default:
		t.Fatalf("unknown mode %q", mode)
	}
}

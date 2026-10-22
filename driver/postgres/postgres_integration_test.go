package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/internal/driverconformance"
)

func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("AIRLOCK_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set AIRLOCK_POSTGRES_DSN for real PostgreSQL tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}
func integrationDriver(t *testing.T, db *sql.DB, prefix string, options ...Option) *Driver {
	t.Helper()
	d, err := New(db, prefix, options...)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	return d
}
func uniquePrefix() string { return "test-" + airlock.NewRequest(time.Now()).Owner }
func executeStatus(t *testing.T, d *Driver, kind airlock.Operation, r airlock.Request, lease string, status airlock.Status) airlock.Result {
	t.Helper()
	got, err := driverconformance.Invoke(context.Background(), d, airlock.Command{Kind: kind, Request: r, LeaseID: lease})
	if err != nil || got.Status != status {
		t.Fatalf("%s got=%+v err=%v want=%s", kind, got, err, status)
	}
	return got
}

func TestIntegrationConformance(t *testing.T) {
	db := integrationDB(t)
	prefix := uniquePrefix()
	driverconformance.Run(t, func(t *testing.T) airlock.Driver { return integrationDriver(t, db, prefix) })
}

func TestIntegrationFirstCreationAndIsolation(t *testing.T) {
	db1, db2 := integrationDB(t), integrationDB(t)
	prefix := uniquePrefix()
	d1, d2 := integrationDriver(t, db1, prefix), integrationDriver(t, db2, prefix)
	r := request(time.Now())
	r.Config.Admission.Capacity = 3
	var wg sync.WaitGroup
	results := make(chan airlock.Result, 12)
	failures := make(chan error, 12)
	for i := 0; i < 12; i++ {
		d := d1
		if i%2 == 1 {
			d = d2
		}
		req := r
		identity := airlock.NewRequest(r.SubmitUntil)
		req.ID, req.Owner = identity.ID, identity.Owner
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := d.Submit(context.Background(), req)
			if err != nil && !errors.Is(err, airlock.ErrQueueFull) && !errors.Is(err, airlock.ErrTenantQueueFull) {
				failures <- err
			}
			results <- got
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	grants := 0
	for got := range results {
		if got.Status == airlock.Granted {
			grants++
		}
	}
	if grants != 3 {
		t.Fatalf("grants=%d want3", grants)
	}
	// Independent namespace columns prevent ambiguous prefix/resource concatenation.
	x := request(time.Now())
	x.Resource = "bc"
	a := integrationDriver(t, db1, prefix+"a")
	b := integrationDriver(t, db2, prefix+"ab")
	executeStatus(t, a, airlock.Submit, x, "", airlock.Granted)
	x.Resource = "c"
	executeStatus(t, b, airlock.Submit, x, "", airlock.Granted)
	// Arbitrary accepted tenant text survives snapshot storage in bytea, and SQL
	// metacharacters are bound data rather than query syntax.
	x = request(time.Now())
	x.Resource = "quote';--"
	x.Tenant = "tenant'\\雪"
	executeStatus(t, a, airlock.Submit, x, "", airlock.Granted)
}

func TestIntegrationPersistentPriorityCursor(t *testing.T) {
	db1, db2 := integrationDB(t), integrationDB(t)
	prefix := uniquePrefix()
	d := integrationDriver(t, db1, prefix)
	holder := request(time.Now())
	holder.Config.Admission.Priority = &airlock.PriorityPolicy{Groups: []airlock.PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 4}, {Name: "low", Turns: 1, MaxQueued: 4}}, DefaultGroup: "low", TenantGroups: map[string]string{"h": "high"}}
	held := executeStatus(t, d, airlock.Submit, holder, "", airlock.Granted)
	makeQueued := func(tenant string) airlock.Request {
		r := holder
		identity := airlock.NewRequest(r.SubmitUntil)
		r.ID, r.Owner, r.Tenant = identity.ID, identity.Owner, tenant
		executeStatus(t, d, airlock.Submit, r, "", airlock.Queued)
		return r
	}
	h1, h2, h3, l1, l2 := makeQueued("h"), makeQueued("h"), makeQueued("h"), makeQueued("l"), makeQueued("l")
	current, lease := holder, held.LeaseID
	for i, next := range []airlock.Request{h1, h2, l1, h3, l2} {
		rebuilt, err := New(db2, prefix)
		if err != nil {
			t.Fatal(err)
		}
		executeStatus(t, rebuilt, airlock.Release, current, lease, airlock.Released)
		got := executeStatus(t, rebuilt, airlock.Observe, next, "", airlock.Granted)
		current, lease = next, got.LeaseID
		if i == 0 {
			if err := db1.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestIntegrationLockWaitDoesNotReviveLease(t *testing.T) {
	db, observer := integrationDB(t), integrationDB(t)
	d := integrationDriver(t, db, uniquePrefix())
	r := request(time.Now())
	r.Config.LeaseTTL = 300 * time.Millisecond
	grant := executeStatus(t, d, airlock.Submit, r, "", airlock.Granted)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var snapshot []byte
	if err := tx.QueryRow(`SELECT snapshot FROM public.airlock_resources WHERE namespace=$1 AND resource=$2 FOR UPDATE`, d.prefix, r.Resource).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	var blocker int
	if err := tx.QueryRow(`SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		got, err := d.Renew(context.Background(), airlock.LeaseRef{Request: r, ID: grant.LeaseID})
		if got.Status == airlock.Granted {
			err = fmt.Errorf("expired lease revived: %+v", got)
		}
		done <- err
	}()
	// Verify the command really is waiting on our lock before letting TTL expire.
	deadline := time.Now().Add(2 * time.Second)
	for {
		var blocked bool
		err := observer.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))`, blocker).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("renew never waited on lock")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(350 * time.Millisecond)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, airlock.ErrLeaseLost) {
		t.Fatalf("renew: %v", err)
	}
	executeStatus(t, d, airlock.Observe, r, "", airlock.Expired)
}

func TestIntegrationCancellationAndConnectionFailure(t *testing.T) {
	db := integrationDB(t)
	d := integrationDriver(t, db, uniquePrefix())
	r := request(time.Now())
	executeStatus(t, d, airlock.Submit, r, "", airlock.Granted)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SELECT 1 FROM public.airlock_resources WHERE namespace=$1 AND resource=$2 FOR UPDATE`, d.prefix, r.Resource); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	got, err := d.Observe(ctx, r)
	cancel()
	if err == nil || got != (airlock.Result{}) {
		t.Fatalf("cancel: %+v %v", got, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	executeStatus(t, d, airlock.Observe, r, "", airlock.Granted)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Observe(context.Background(), r); err == nil || got != (airlock.Result{}) {
		t.Fatalf("closed connection: %+v %v", got, err)
	}
}

func TestIntegrationAbandonedClientsReclaimed(t *testing.T) {
	db1, db2 := integrationDB(t), integrationDB(t)
	prefix := uniquePrefix()
	d1, d2 := integrationDriver(t, db1, prefix), integrationDriver(t, db2, prefix)
	holder := request(time.Now())
	holder.Config.LeaseTTL = 100 * time.Millisecond
	executeStatus(t, d1, airlock.Submit, holder, "", airlock.Granted)
	queued := holder
	identity := airlock.NewRequest(holder.SubmitUntil)
	queued.ID, queued.Owner = identity.ID, identity.Owner
	queued.WaitUntil = time.Now().Add(50 * time.Millisecond).UTC().Truncate(time.Millisecond)
	executeStatus(t, d1, airlock.Submit, queued, "", airlock.Queued)
	// Closing a client has no special release path; recovery relies only on backend
	// TTL and commands from surviving clients, just like an abruptly exited process.
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	executeStatus(t, d2, airlock.Observe, holder, "", airlock.Expired)
	executeStatus(t, d2, airlock.Observe, queued, "", airlock.Expired)
	fresh := holder
	identity = airlock.NewRequest(holder.SubmitUntil)
	fresh.ID, fresh.Owner = identity.ID, identity.Owner
	executeStatus(t, d2, airlock.Submit, fresh, "", airlock.Granted)
}

func TestIntegrationTerminatedConnectionFailsClosed(t *testing.T) {
	db, admin := integrationDB(t), integrationDB(t)
	d := integrationDriver(t, db, uniquePrefix())
	r := request(time.Now())
	r.Config.LeaseTTL = 10 * time.Second
	held := executeStatus(t, d, airlock.Submit, r, "", airlock.Granted)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SELECT 1 FROM public.airlock_resources WHERE namespace=$1 AND resource=$2 FOR UPDATE`, d.prefix, r.Resource); err != nil {
		t.Fatal(err)
	}
	var blocker int
	if err := tx.QueryRow(`SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	type response struct {
		result airlock.Result
		err    error
	}
	done := make(chan response, 1)
	go func() {
		got, err := d.Renew(context.Background(), airlock.LeaseRef{Request: r, ID: held.LeaseID})
		done <- response{got, err}
	}()
	var target int
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := admin.QueryRow(`SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)) LIMIT 1`, blocker).Scan(&target)
		if err == nil {
			break
		}
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("command never waited on lock")
		}
		time.Sleep(time.Millisecond)
	}
	var terminated bool
	if err := admin.QueryRow(`SELECT pg_terminate_backend($1)`, target).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate=%t err=%v", terminated, err)
	}
	got := <-done
	if got.err == nil || got.result != (airlock.Result{}) {
		t.Fatalf("terminated backend returned usable result: %+v err=%v", got.result, got.err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	recovered := executeStatus(t, d, airlock.Observe, r, "", airlock.Granted)
	if recovered.LeaseID != held.LeaseID || recovered.Remaining > held.Remaining {
		t.Fatalf("failed command changed lease: before=%+v after=%+v", held, recovered)
	}
}

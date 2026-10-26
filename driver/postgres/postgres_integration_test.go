package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"

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

func integrationDriver(t *testing.T, db *sql.DB, namespace string, options ...Option) *Driver {
	t.Helper()
	d, err := New(db, namespace, options...)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	return d
}

func uniqueNamespace() string { return "test-" + airlock.NewRequest(time.Now()).Owner }

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
	namespace := uniqueNamespace()
	driverconformance.Run(t, func(t *testing.T) airlock.Driver { return integrationDriver(t, db, namespace) })
}

func TestIntegrationFirstCreationAndIsolation(t *testing.T) {
	db1, db2 := integrationDB(t), integrationDB(t)
	namespace := uniqueNamespace()
	d1, d2 := integrationDriver(t, db1, namespace), integrationDriver(t, db2, namespace)
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
	// Independent namespace columns prevent ambiguous namespace/resource concatenation.
	x := request(time.Now())
	x.Resource = "bc"
	a := integrationDriver(t, db1, namespace+"a")
	b := integrationDriver(t, db2, namespace+"ab")
	executeStatus(t, a, airlock.Submit, x, "", airlock.Granted)
	x.Resource = "c"
	executeStatus(t, b, airlock.Submit, x, "", airlock.Granted)
	// Arbitrary accepted tenant text survives relational storage, and SQL
	// metacharacters are bound data rather than query syntax.
	x = request(time.Now())
	x.Resource = "quote';--"
	x.Tenant = "tenant'\\雪"
	executeStatus(t, a, airlock.Submit, x, "", airlock.Granted)
}

func TestIntegrationPersistentPriorityCursor(t *testing.T) {
	db1, db2 := integrationDB(t), integrationDB(t)
	namespace := uniqueNamespace()
	d := integrationDriver(t, db1, namespace)
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
		rebuilt, err := New(db2, namespace)
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
	d := integrationDriver(t, db, uniqueNamespace())
	r := request(time.Now())
	r.Config.LeaseTTL = 300 * time.Millisecond
	grant := executeStatus(t, d, airlock.Submit, r, "", airlock.Granted)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var revision string
	if err := tx.QueryRow(`SELECT revision FROM public.airlock_resources WHERE namespace=$1 AND resource=$2 FOR UPDATE`, d.namespace, r.Resource).Scan(&revision); err != nil {
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
	d := integrationDriver(t, db, uniqueNamespace())
	r := request(time.Now())
	grant := executeStatus(t, d, airlock.Submit, r, "", airlock.Granted)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SELECT 1 FROM public.airlock_resources WHERE namespace=$1 AND resource=$2 FOR UPDATE`, d.namespace, r.Resource); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	got, err := d.Renew(ctx, airlock.LeaseRef{Request: r, ID: grant.LeaseID})
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
	namespace := uniqueNamespace()
	d1, d2 := integrationDriver(t, db1, namespace), integrationDriver(t, db2, namespace)
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
	d := integrationDriver(t, db, uniqueNamespace())
	r := request(time.Now())
	r.Config.LeaseTTL = 10 * time.Second
	held := executeStatus(t, d, airlock.Submit, r, "", airlock.Granted)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SELECT 1 FROM public.airlock_resources WHERE namespace=$1 AND resource=$2 FOR UPDATE`, d.namespace, r.Resource); err != nil {
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

func TestIntegrationShared(t *testing.T) {
	d := integrationDriver(t, integrationDB(t), uniqueNamespace())
	client, err := airlock.NewShared(d, airlock.SharedOptions{Resource: "client-lifecycle", Config: airlock.ResourceConfig{Admission: airlock.Config{Capacity: 2, MaxQueued: 4, MaxQueuedPerTenant: 4}, LeaseTTL: 5 * time.Second, MaxWait: time.Second, SubmitWindow: time.Second, Retention: time.Second, MaxRecords: 30}, OperationTimeout: time.Second, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	acquisition, cancel := context.WithCancel(context.Background())
	lease, err := client.AcquireNFor(acquisition, "online", 2)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if !lease.Valid() {
		t.Fatal("acquisition context revoked delivered lease")
	}
	waiting, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = client.AcquireFor(waiting, "batch")
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded wait: %v", err)
	}
	if err := lease.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lease.Done():
	default:
		t.Fatal("released lease still usable")
	}
	replacement, err := client.AcquireN(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationRelationalDeltaWrites(t *testing.T) {
	db := integrationDB(t)
	d := integrationDriver(t, db, uniqueNamespace())
	first := request(time.Now())
	first.Config.Admission.Capacity = 2
	first.Config.LeaseTTL = 30 * time.Second
	a := executeStatus(t, d, airlock.Submit, first, "", airlock.Granted)
	second := first
	id := airlock.NewRequest(first.SubmitUntil)
	second.ID, second.Owner = id.ID, id.Owner
	b := executeStatus(t, d, airlock.Submit, second, "", airlock.Granted)
	readVersion := func(r airlock.Request) string {
		var version string
		if err := db.QueryRow(`SELECT xmin::text FROM public.airlock_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.namespace, r.Resource, r.ID).Scan(&version); err != nil {
			t.Fatal(err)
		}
		return version
	}
	va, vb := readVersion(first), readVersion(second)
	// Polling changes the resource revision but must not rewrite either request.
	executeStatus(t, d, airlock.Observe, first, "", airlock.Granted)
	if readVersion(first) != va || readVersion(second) != vb {
		t.Fatal("Observe rewrote unchanged requests")
	}
	time.Sleep(2 * time.Millisecond)
	executeStatus(t, d, airlock.Renew, second, b.LeaseID, airlock.Granted)
	if readVersion(first) != va || readVersion(second) == vb {
		t.Fatal("Renew did not restrict writes to its request")
	}
	vb = readVersion(second)
	executeStatus(t, d, airlock.Release, first, a.LeaseID, airlock.Released)
	if readVersion(second) != vb {
		t.Fatal("Release rewrote unrelated request")
	}
	var tenant, status string
	var weight int
	if err := db.QueryRow(`SELECT tenant,weight,status FROM public.airlock_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.namespace, first.Resource, first.ID).Scan(&tenant, &weight, &status); err != nil {
		t.Fatal(err)
	}
	if tenant != first.Tenant || weight != first.Weight || status != "released" {
		t.Fatal(tenant, weight, status)
	}
}

func TestIntegrationMissingColumnsRejected(t *testing.T) {
	db := integrationDB(t)
	name := "incomplete_" + airlock.NewRequest(time.Now()).Owner
	d, err := New(db, "", WithTable(name), WithOperationTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s(namespace text COLLATE "C" NOT NULL,resource text COLLATE "C" NOT NULL,PRIMARY KEY(namespace,resource))`, d.resourceTable)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, d.requestTable))
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, d.resourceTable))
	})
	if err = d.EnsureSchema(context.Background()); err == nil || !strings.Contains(err.Error(), "incompatible schema") {
		t.Fatalf("incomplete schema silently accepted: %v", err)
	}
}

func TestIntegrationDequeuePreservesFollowerRows(t *testing.T) {
	db := integrationDB(t)
	d := integrationDriver(t, db, uniqueNamespace())
	holder := request(time.Now())
	holder.Config.LeaseTTL = 30 * time.Second
	held := executeStatus(t, d, airlock.Submit, holder, "", airlock.Granted)
	var queued []airlock.Request
	for i := 0; i < 3; i++ {
		r := holder
		id := airlock.NewRequest(r.SubmitUntil)
		r.ID, r.Owner = id.ID, id.Owner
		executeStatus(t, d, airlock.Submit, r, "", airlock.Queued)
		queued = append(queued, r)
	}
	var version string
	tail := queued[2]
	if err := db.QueryRow(`SELECT xmin::text FROM public.airlock_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.namespace, tail.Resource, tail.ID).Scan(&version); err != nil {
		t.Fatal(err)
	}
	executeStatus(t, d, airlock.Release, holder, held.LeaseID, airlock.Released)
	var after string
	var position int64
	if err := db.QueryRow(`SELECT xmin::text,queue_position FROM public.airlock_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.namespace, tail.Resource, tail.ID).Scan(&after, &position); err != nil {
		t.Fatal(err)
	}
	if after != version || position != 2 {
		t.Fatalf("follower rewritten/reindexed: %s -> %s position %d", version, after, position)
	}
	// Reading a queue whose first position is no longer zero must preserve FIFO.
	for i := 0; i < 2; i++ {
		got := executeStatus(t, d, airlock.Observe, queued[i], "", airlock.Granted)
		executeStatus(t, d, airlock.Release, queued[i], got.LeaseID, airlock.Released)
	}
	executeStatus(t, d, airlock.Observe, tail, "", airlock.Granted)
}

// The database really commits; only its driver-level acknowledgement is dropped.
// This is a deterministic reply-loss boundary, not a TCP partition simulation.
type replyLossConnector struct {
	driver.Connector
	armed atomic.Bool
}

func (c *replyLossConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &replyLossConn{Conn: conn, armed: &c.armed}, nil
}

type replyLossConn struct {
	driver.Conn
	armed *atomic.Bool
}

func (c *replyLossConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return &replyLossTx{Tx: tx, armed: c.armed}, nil
}

func (c *replyLossConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *replyLossConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type replyLossTx struct {
	driver.Tx
	armed *atomic.Bool
}

func (tx *replyLossTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.armed.Swap(false) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func TestIntegrationCommittedResponseLoss(t *testing.T) {
	normalDB := integrationDB(t)
	base, err := stdlib.GetDefaultDriver().(driver.DriverContext).OpenConnector(os.Getenv("AIRLOCK_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	connector := &replyLossConnector{Connector: base}
	faultyDB := sql.OpenDB(connector)
	t.Cleanup(func() { _ = faultyDB.Close() })
	namespace := uniqueNamespace()
	faulty := integrationDriver(t, faultyDB, namespace)
	observer := integrationDriver(t, normalDB, namespace)
	r := request(time.Now())
	r.Config.LeaseTTL = 5 * time.Second
	r.Config.Admission.Capacity = 2
	r.Weight = 2
	unknown := func(kind airlock.Operation, lease string) {
		t.Helper()
		connector.armed.Store(true)
		result, e := driverconformance.Invoke(context.Background(), faulty, airlock.Command{Kind: kind, Request: r, LeaseID: lease})
		if !errors.Is(e, io.ErrUnexpectedEOF) || result != (airlock.Result{}) {
			t.Fatalf("unknown %s exposed result: %+v %v", kind, result, e)
		}
	}
	unknown(airlock.Submit, "")
	grant := executeStatus(t, observer, airlock.Observe, r, "", airlock.Granted)
	retry := executeStatus(t, observer, airlock.Submit, r, "", airlock.Granted)
	if retry.LeaseID != grant.LeaseID || retry.Remaining > grant.Remaining {
		t.Fatal("lost Submit response caused a second grant/extended TTL")
	}
	other := r
	identity := airlock.NewRequest(r.SubmitUntil)
	other.ID, other.Owner = identity.ID, identity.Owner
	other.Weight = 1
	executeStatus(t, observer, airlock.Submit, other, "", airlock.Queued)
	unknown(airlock.Renew, grant.LeaseID)
	renewed := executeStatus(t, observer, airlock.Observe, r, "", airlock.Granted)
	if renewed.LeaseID != grant.LeaseID {
		t.Fatal("renew changed identity")
	}
	unknown(airlock.Release, grant.LeaseID)
	executeStatus(t, observer, airlock.Release, r, grant.LeaseID, airlock.Released)
	next := executeStatus(t, observer, airlock.Observe, other, "", airlock.Granted)
	executeStatus(t, observer, airlock.Release, other, next.LeaseID, airlock.Released)
}

func TestIntegrationObserveDoesNotLockOrWrite(t *testing.T) {
	db := integrationDB(t)
	d := integrationDriver(t, db, uniqueNamespace())
	r := request(time.Now())
	r.Config.LeaseTTL = time.Minute
	grant := executeStatus(t, d, airlock.Submit, r, "", airlock.Granted)
	pending := r
	id := airlock.NewRequest(r.SubmitUntil)
	pending.ID, pending.Owner, pending.Tenant = id.ID, id.Owner, "waiting"
	queued := executeStatus(t, d, airlock.Submit, pending, "", airlock.Queued)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SELECT resource FROM public.airlock_resources WHERE namespace=$1 AND resource=$2 FOR UPDATE`, d.namespace, r.Resource); err != nil {
		t.Fatal(err)
	}
	// A poll on this same resource must succeed while its mutation lock is held.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range 10 {
		result, err := d.Observe(ctx, pending)
		if err != nil || result.Status != airlock.Queued || result.Revision != queued.Revision {
			t.Fatalf("%+v %v", result, err)
		}
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var revision string
	if err := db.QueryRow(`SELECT revision FROM public.airlock_resources WHERE namespace=$1 AND resource=$2`, d.namespace, r.Resource).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if revision != fmt.Sprint(queued.Revision) {
		t.Fatal("read-only polling wrote resource metadata")
	}
	executeStatus(t, d, airlock.Release, r, grant.LeaseID, airlock.Released)
	executeStatus(t, d, airlock.Observe, pending, "", airlock.Granted)
}

func BenchmarkIntegrationObserveQueued(b *testing.B) { benchmarkIntegrationObservation(b, false) }

func BenchmarkIntegrationRenewWithWaiters(b *testing.B) { benchmarkIntegrationObservation(b, true) }

func benchmarkIntegrationObservation(b *testing.B, renew bool) {
	dsn := os.Getenv("AIRLOCK_POSTGRES_DSN")
	if dsn == "" {
		b.Skip("set AIRLOCK_POSTGRES_DSN")
	}
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			namespace := "observe-bench-" + airlock.NewRequest(time.Now()).Owner
			d, err := New(db, namespace)
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			if err := d.EnsureSchema(ctx); err != nil {
				b.Fatal(err)
			}
			defer func() {
				_, _ = db.Exec(`DELETE FROM public.airlock_requests WHERE namespace=$1`, namespace)
				_, _ = db.Exec(`DELETE FROM public.airlock_resources WHERE namespace=$1`, namespace)
			}()
			r := request(time.Now())
			r.Config.LeaseTTL, r.Config.MaxWait, r.Config.SubmitWindow = time.Hour, time.Hour, time.Hour
			r.Config.Admission.MaxQueued, r.Config.Admission.MaxQueuedPerTenant, r.Config.MaxRecords = 1000, 1000, 1001
			r.SubmitUntil, r.WaitUntil = time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond), time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
			id := airlock.NewRequest(r.SubmitUntil)
			r.ID, r.Owner = id.ID, id.Owner
			held, err := d.Submit(ctx, r)
			if err != nil {
				b.Fatal(err)
			}
			var pending airlock.Request
			for range count {
				pending = r
				id := airlock.NewRequest(r.SubmitUntil)
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

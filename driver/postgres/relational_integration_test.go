package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestIntegrationRelationalDeltaWrites(t *testing.T) {
	db := integrationDB(t)
	d := integrationDriver(t, db, uniquePrefix())
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
		if err := db.QueryRow(`SELECT xmin::text FROM public.airlock_resources_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.prefix, r.Resource, r.ID).Scan(&version); err != nil {
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
	if err := db.QueryRow(`SELECT tenant,weight,status FROM public.airlock_resources_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.prefix, first.Resource, first.ID).Scan(&tenant, &weight, &status); err != nil {
		t.Fatal(err)
	}
	if tenant != first.Tenant || weight != first.Weight || status != "released" {
		t.Fatal(tenant, weight, status)
	}
}

func legacyDriver(t *testing.T, db *sql.DB) *Driver {
	t.Helper()
	name := "migration_" + airlock.NewRequest(time.Now()).Owner
	d, err := New(db, "", WithTable(name), WithOperationTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s(namespace text COLLATE "C" NOT NULL,resource text COLLATE "C" NOT NULL,snapshot bytea NOT NULL,PRIMARY KEY(namespace,resource))`, d.table)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, d.requests))
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, d.table))
	})
	return d
}

func TestIntegrationSnapshotMigration(t *testing.T) {
	db := integrationDB(t)
	d := legacyDriver(t, db)
	now := time.Now().UTC().Truncate(time.Millisecond)
	holder := request(now)
	holder.Config.LeaseTTL = 30 * time.Second
	snapshot, held, err := airlock.Transition(nil, now, airlock.Command{Kind: airlock.Submit, Request: holder})
	if err != nil {
		t.Fatal(err)
	}
	waiter := holder
	id := airlock.NewRequest(holder.SubmitUntil)
	waiter.ID, waiter.Owner = id.ID, id.Owner
	snapshot, _, err = airlock.Transition(snapshot, now, airlock.Command{Kind: airlock.Submit, Request: waiter})
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "another namespace"} {
		if _, err = db.Exec(fmt.Sprintf(`INSERT INTO %s VALUES($1,$2,$3)`, d.table), prefix, holder.Resource, snapshot); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.EnsureSchema(context.Background()); err == nil || !strings.Contains(err.Error(), "offline migration") {
		t.Fatalf("legacy schema silently accepted: %v", err)
	}
	if err = d.MigrateSnapshotSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = d.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, d.requests)).Scan(&count); err != nil || count != 4 {
		t.Fatal(count, err)
	}
	var revision string
	var observed int64
	if err = db.QueryRow(fmt.Sprintf(`SELECT revision,observed_ms FROM %s WHERE namespace=''`, d.table)).Scan(&revision, &observed); err != nil || revision != "2" || observed != now.UnixMilli() {
		t.Fatal(revision, observed, err)
	}
	if err = d.MigrateSnapshotSchema(context.Background()); err == nil {
		t.Fatal("repeated migration accepted")
	}
	got := executeStatus(t, d, airlock.Observe, holder, "", airlock.Granted)
	if got.LeaseID != held.LeaseID || got.Remaining > held.Remaining {
		t.Fatal("migration reset lease")
	}
	executeStatus(t, d, airlock.Observe, waiter, "", airlock.Queued)
	executeStatus(t, d, airlock.Release, holder, held.LeaseID, airlock.Released)
	executeStatus(t, d, airlock.Observe, waiter, "", airlock.Granted)
	other := *d
	other.prefix = "another namespace"
	executeStatus(t, &other, airlock.Observe, holder, "", airlock.Granted)
}

func TestIntegrationMigrationRollback(t *testing.T) {
	db := integrationDB(t)
	d := legacyDriver(t, db)
	r := request(time.Now())
	snapshot, _, err := airlock.Transition(nil, time.Now(), airlock.Command{Kind: airlock.Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}
	for prefix, data := range map[string][]byte{"a": snapshot, "z": []byte(`{}`)} {
		if _, err = db.Exec(fmt.Sprintf(`INSERT INTO %s VALUES($1,$2,$3)`, d.table), prefix, r.Resource, data); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.MigrateSnapshotSchema(context.Background()); err == nil {
		t.Fatal("corrupt migration succeeded")
	}
	var stored []byte
	if err = db.QueryRow(fmt.Sprintf(`SELECT snapshot FROM %s WHERE namespace='a'`, d.table)).Scan(&stored); err != nil || string(stored) != string(snapshot) {
		t.Fatal("legacy data changed", err)
	}
	var relation sql.NullString
	if err = db.QueryRow(`SELECT to_regclass($1)::text`, d.requests).Scan(&relation); err != nil || relation.Valid {
		t.Fatal("partial request table survived", err)
	}
}

func TestIntegrationDequeuePreservesFollowerRows(t *testing.T) {
	db := integrationDB(t)
	d := integrationDriver(t, db, uniquePrefix())
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
	if err := db.QueryRow(`SELECT xmin::text FROM public.airlock_resources_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.prefix, tail.Resource, tail.ID).Scan(&version); err != nil {
		t.Fatal(err)
	}
	executeStatus(t, d, airlock.Release, holder, held.LeaseID, airlock.Released)
	var after string
	var position int64
	if err := db.QueryRow(`SELECT xmin::text,queue_position FROM public.airlock_resources_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.prefix, tail.Resource, tail.ID).Scan(&after, &position); err != nil {
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

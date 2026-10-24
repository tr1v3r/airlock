package postgres

import (
	"context"
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

func TestIntegrationMissingColumnsRejected(t *testing.T) {
	db := integrationDB(t)
	name := "incomplete_" + airlock.NewRequest(time.Now()).Owner
	d, err := New(db, "", WithTable(name), WithOperationTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s(namespace text COLLATE "C" NOT NULL,resource text COLLATE "C" NOT NULL,PRIMARY KEY(namespace,resource))`, d.table)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, d.requests))
		_, _ = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, d.table))
	})
	if err = d.EnsureSchema(context.Background()); err == nil || !strings.Contains(err.Error(), "incompatible schema") {
		t.Fatalf("incomplete schema silently accepted: %v", err)
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

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tr1v3r/airlock/protocol"
)

func TestIntegrationRelationalDeltaWrites(t *testing.T) {
	db := integrationDB(t)
	d := integrationDriver(t, db, uniquePrefix())
	first := request(time.Now())
	first.Config.Admission.Capacity = 2
	first.Config.LeaseTTL = 30 * time.Second
	a := executeStatus(t, d, protocol.Submit, first, "", protocol.Granted)
	second := first
	id := protocol.NewRequest(first.SubmitUntil)
	second.ID, second.Owner = id.ID, id.Owner
	b := executeStatus(t, d, protocol.Submit, second, "", protocol.Granted)
	readVersion := func(r protocol.Request) string {
		var version string
		if err := db.QueryRow(`SELECT xmin::text FROM public.airlock_resources_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.prefix, r.Resource, r.ID).Scan(&version); err != nil {
			t.Fatal(err)
		}
		return version
	}
	va, vb := readVersion(first), readVersion(second)
	// Polling changes the resource revision but must not rewrite either request.
	executeStatus(t, d, protocol.Observe, first, "", protocol.Granted)
	if readVersion(first) != va || readVersion(second) != vb {
		t.Fatal("Observe rewrote unchanged requests")
	}
	time.Sleep(2 * time.Millisecond)
	executeStatus(t, d, protocol.Renew, second, b.LeaseID, protocol.Granted)
	if readVersion(first) != va || readVersion(second) == vb {
		t.Fatal("Renew did not restrict writes to its request")
	}
	vb = readVersion(second)
	executeStatus(t, d, protocol.Release, first, a.LeaseID, protocol.Released)
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
	name := "incomplete_" + protocol.NewRequest(time.Now()).Owner
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
	held := executeStatus(t, d, protocol.Submit, holder, "", protocol.Granted)
	var queued []protocol.Request
	for i := 0; i < 3; i++ {
		r := holder
		id := protocol.NewRequest(r.SubmitUntil)
		r.ID, r.Owner = id.ID, id.Owner
		executeStatus(t, d, protocol.Submit, r, "", protocol.Queued)
		queued = append(queued, r)
	}
	var version string
	tail := queued[2]
	if err := db.QueryRow(`SELECT xmin::text FROM public.airlock_resources_requests WHERE namespace=$1 AND resource=$2 AND id=$3`, d.prefix, tail.Resource, tail.ID).Scan(&version); err != nil {
		t.Fatal(err)
	}
	executeStatus(t, d, protocol.Release, holder, held.LeaseID, protocol.Released)
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
		got := executeStatus(t, d, protocol.Observe, queued[i], "", protocol.Granted)
		executeStatus(t, d, protocol.Release, queued[i], got.LeaseID, protocol.Released)
	}
	executeStatus(t, d, protocol.Observe, tail, "", protocol.Granted)
}

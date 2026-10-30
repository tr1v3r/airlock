package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestIntegrationFreshHistoryAndGC(t *testing.T) {
	db := integrationDB(t)
	ctx := context.Background()
	d := integrationDriver(t, db, uniqueNamespace())
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	r.Config.MaxRecords = 1002
	r.Config.Retention = time.Minute
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, before, err := d.load(ctx, tx, r)
	if err != nil {
		t.Fatal(err)
	}
	state, _, _, err := airlock.Transition(nil, now, airlock.Command{Kind: airlock.Cancel, Request: r})
	if err != nil {
		t.Fatal(err)
	}
	old := r
	base := airlock.NewRequest(now.Add(-2 * time.Minute))
	old.ID, old.Owner, old.SubmitUntil, old.WaitUntil = base.ID, base.Owner, base.SubmitUntil, base.SubmitUntil
	state.Records = map[string]*airlock.DriverRecord{old.ID: {Request: old, Status: airlock.Cancelled, Finished: now}}
	for i := 0; i < 1000; i++ {
		q := old
		q.ID = airlock.NewRequest(old.SubmitUntil).ID
		state.Records[q.ID] = &airlock.DriverRecord{Request: q, Status: airlock.Cancelled, Finished: now}
	}
	if err = d.persist(ctx, tx, state, before); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	fresh := r
	fresh.ID = airlock.NewRequest(r.SubmitUntil).ID
	grant, err := d.Submit(ctx, fresh)
	if err != nil {
		t.Fatal(err)
	}
	var version, total, indexed int
	if err = db.QueryRow("SELECT state_version,total_records FROM "+d.resourceTable+" WHERE namespace=$1", d.namespace).Scan(&version, &total); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT count(*) FROM "+d.requestTable+" WHERE namespace=$1 AND gc_after_ms IS NOT NULL", d.namespace).Scan(&indexed); err != nil {
		t.Fatal(err)
	}
	if version != 1 || total != 1002 || indexed != 1001 {
		t.Fatalf("version=%d total=%d indexed=%d", version, total, indexed)
	}

	if _, err = db.Exec("ANALYZE " + d.requestTable); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"SELECT " + requestColumns + " FROM " + d.requestTable + " WHERE namespace=$1 AND resource=$2 AND status IN ('queued','granted') UNION ALL SELECT " + requestColumns + " FROM " + d.requestTable + " WHERE namespace=$1 AND resource=$2 AND id=$3 AND status NOT IN ('queued','granted') ORDER BY tenant,queue_position NULLS LAST,id LIMIT 1000001",
		"DELETE FROM " + d.requestTable + " WHERE namespace=$1 AND resource=$2 AND status NOT IN ('queued','granted') AND gc_after_ms<=0 AND id<>$3",
	} {
		planRows, e := db.Query("EXPLAIN "+query, d.namespace, r.Resource, old.ID)
		if e != nil {
			t.Fatal(e)
		}
		var plan strings.Builder
		for planRows.Next() {
			var line string
			if e = planRows.Scan(&line); e != nil {
				t.Fatal(e)
			}
			plan.WriteString(line + "\n")
		}
		e = planRows.Err()
		if err := planRows.Close(); err != nil {
			t.Fatal(err)
		}
		if e != nil {
			t.Fatal(e)
		}
		t.Log(plan.String())
		if strings.Contains(plan.String(), "Seq Scan") {
			t.Fatalf("history query scans full table: %s", plan.String())
		}
	}
	// The hot path only materializes the active request and exact target terminal.
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var stored storedHistory
	partial, _, err := d.loadState(ctx, tx, r.Resource, nil, old.ID, &stored)
	if err != nil || len(partial.Records) != 2 || stored.total != 1002 {
		t.Fatalf("partial=%+v metadata=%+v error=%v", partial, stored, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = d.Renew(ctx, airlock.LeaseRef{Request: fresh, ID: grant.LeaseID}); err != nil {
		t.Fatal(err)
	}
	another := r
	another.ID = airlock.NewRequest(r.SubmitUntil).ID
	if _, err = d.Submit(ctx, another); !errors.Is(err, airlock.ErrHistoryFull) {
		t.Fatalf("full history: %v", err)
	}

	// A touched terminal's index must match its authoritative request timestamps.
	if _, err = db.Exec("UPDATE "+d.requestTable+" SET gc_after_ms=gc_after_ms+1 WHERE namespace=$1 AND id=$2", d.namespace, old.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = d.Cancel(ctx, old); !errors.Is(err, airlock.ErrCorruptState) {
		t.Fatalf("mismatched target index: %v", err)
	}
	if _, err = db.Exec("UPDATE "+d.requestTable+" SET gc_after_ms=gc_after_ms-1 WHERE namespace=$1 AND id=$2", d.namespace, old.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("UPDATE "+d.requestTable+" SET gc_after_ms=0 WHERE namespace=$1 AND id=$2", d.namespace, fresh.ID); err == nil {
		t.Fatal("active GC index accepted")
	}
	// Make all history due without waiting. All immutable timestamps remain valid
	// relative to finished: this models a large simultaneous expiry event.
	past := now.Add(-2 * time.Minute).UnixMilli()
	if _, err = db.Exec(fmt.Sprintf("UPDATE %s SET finished_ms=$2,gc_after_ms=$3 WHERE namespace=$1 AND gc_after_ms IS NOT NULL", d.requestTable), d.namespace, past, now.Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	// The target ID must still conflict before GC. Deleting other due records first
	// is harmless only if the entire transaction rolls back on this nil transition.
	conflict := old
	conflict.Owner = airlock.NewRequest(now).Owner
	if _, err = d.Submit(ctx, conflict); !errors.Is(err, airlock.ErrRequestConflict) {
		t.Fatalf("expired target conflict: %v", err)
	}
	if err = db.QueryRow("SELECT count(*) FROM "+d.requestTable+" WHERE namespace=$1", d.namespace).Scan(&total); err != nil || total != 1002 {
		t.Fatalf("rollback count=%d err=%v", total, err)
	}
	if _, err = d.Submit(ctx, another); err != nil {
		t.Fatalf("due history must free capacity beyond a GC batch: %v", err)
	}
	if err = db.QueryRow("SELECT total_records FROM "+d.resourceTable+" WHERE namespace=$1", d.namespace).Scan(&total); err != nil || total != 2 {
		t.Fatalf("after GC count=%d err=%v", total, err)
	}
}

func TestIntegrationHistoryRequiresFreshSchema(t *testing.T) {
	db := integrationDB(t)
	ctx := context.Background()
	d := integrationDriver(t, db, uniqueNamespace(), WithTable("fresh_"+airlock.NewRequest(time.Now()).Owner[:12]))
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE " + d.requestTable)
		_, _ = db.Exec("DROP TABLE " + d.resourceTable)
	})
	// Missing required layout is rejected, never silently migrated or backfilled.
	if _, err := db.Exec("ALTER TABLE " + d.resourceTable + " DROP COLUMN total_records"); err != nil {
		t.Fatal(err)
	}
	if err := d.EnsureSchema(ctx); err == nil {
		t.Fatal("incompatible layout accepted")
	}
	var exists bool
	if err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid=$1::regclass AND attname='total_records' AND NOT attisdropped)", d.resourceTable).Scan(&exists); err != nil || exists {
		t.Fatal(exists, err)
	}
}

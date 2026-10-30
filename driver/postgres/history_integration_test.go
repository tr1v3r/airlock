package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestIntegrationPartitionedHistoryMigrationAndGC(t *testing.T) {
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
	// Restore v1 on-disk metadata to exercise the one-time complete validation/backfill.
	if _, err = db.Exec("UPDATE "+d.resourceTable+" SET state_version=1,total_records=NULL WHERE namespace=$1", d.namespace); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("UPDATE "+d.requestTable+" SET gc_after_ms=NULL WHERE namespace=$1", d.namespace); err != nil {
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
	if version != 2 || total != 1002 || indexed != 1001 {
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
	// A legacy writer sees the marker and fails before any mutation.
	state.Version = version
	if next, _, _, err := airlock.Transition(state, now, airlock.Command{Kind: airlock.Submit, Request: fresh}); next != nil || !errors.Is(err, airlock.ErrCorruptState) {
		t.Fatalf("legacy guard: %v %v", next, err)
	}
	// The hot path only materializes the active request and exact target terminal.
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var stored storedHistory
	partial, _, err := d.loadState(ctx, tx, r.Resource, nil, old.ID, &stored)
	if err != nil || len(partial.Records) != 2 || !stored.partitioned || stored.total != 1002 {
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

func TestIntegrationLegacyHistorySchemaUpgrade(t *testing.T) {
	db := integrationDB(t)
	ctx := context.Background()
	d, err := New(db, uniqueNamespace(), WithTable("legacy_"+airlock.NewRequest(time.Now()).Owner[:12]))
	if err != nil {
		t.Fatal(err)
	}
	ddl := d.schema()
	ddl[0] = strings.Replace(ddl[0], "state_version IN (1,2)", "state_version=1", 1)
	for _, q := range ddl[:2] {
		if _, err = db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE " + d.requestTable)
		_, _ = db.Exec("DROP TABLE " + d.resourceTable)
	})
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	config, err := json.Marshal(r.Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO "+d.resourceTable+" (namespace,resource,config,capacity,revision,observed_ms) VALUES($1,$2,$3,$4,1,$5)", d.namespace, r.Resource, string(config), r.Config.Admission.Capacity, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO "+d.requestTable+" (namespace,resource,id,owner,tenant,weight,submit_until_ms,wait_until_ms,status,lease_id,finished_ms,failure) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'cancelled','',$9,'')", d.namespace, r.Resource, r.ID, r.Owner, r.Tenant, r.Weight, r.SubmitUntil.UnixMilli(), r.WaitUntil.UnixMilli(), now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = d.EnsureSchema(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// A command, not DDL, migrates and validates each resource's persisted state.
	out, err := d.Cancel(ctx, r)
	if err != nil || out.Status != airlock.Cancelled {
		t.Fatal(out, err)
	}
	var version, count int
	if err = db.QueryRow("SELECT state_version,total_records FROM "+d.resourceTable+" WHERE namespace=$1", d.namespace).Scan(&version, &count); err != nil || version != 2 || count != 1 {
		t.Fatal(version, count, err)
	}
	var gc int64
	if err = db.QueryRow("SELECT gc_after_ms FROM "+d.requestTable+" WHERE namespace=$1", d.namespace).Scan(&gc); err != nil || gc != r.SubmitUntil.UnixMilli() {
		t.Fatal(gc, err)
	}
}

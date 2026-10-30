package postgres

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/tr1v3r/airlock"
)

func TestPersistBatchesBoundParametersAndCheckCounts(t *testing.T) {
	for _, badCount := range []bool{false, true} {
		t.Run(fmt.Sprint(badCount), func(t *testing.T) {
			d, m := mockDriver(t)
			m.ExpectBegin()
			tx, err := d.db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			now := time.Now().UTC().Truncate(time.Millisecond)
			r := request(now)
			s := &airlock.SharedState{Resource: r.Resource, Config: r.Config, Time: now, Revision: 2, Records: make(map[string]*airlock.DriverRecord)}
			before := make(map[string]row)
			for i := 0; i < 513; i++ {
				id := fmt.Sprint(i)
				before["old"+id] = row{}
				rec := &airlock.DriverRecord{Request: r, Status: airlock.Cancelled, Finished: now}
				rec.Request.ID = "new" + id
				s.Records[rec.Request.ID] = rec
			}
			m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
			for _, n := range []int{256, 256, 1} {
				args := make([]driver.Value, n+2)
				for i := range args {
					args[i] = sqlmock.AnyArg()
				}
				count := int64(n)
				if badCount {
					count--
				}
				m.ExpectExec("DELETE.*id IN").WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, count))
				if badCount {
					break
				}
			}
			if !badCount {
				for _, n := range []int{256, 256, 1} {
					args := make([]driver.Value, n*15)
					for i := range args {
						args[i] = sqlmock.AnyArg()
					}
					m.ExpectExec("INSERT.*VALUES").WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, int64(n)))
				}
			}
			m.ExpectRollback()
			err = d.persist(context.Background(), tx, s, before)
			if (err != nil) != badCount {
				t.Fatalf("badCount=%v err=%v", badCount, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := m.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPersistBatchUpsertAffectedCount(t *testing.T) {
	d, m := mockDriver(t)
	m.ExpectBegin()
	tx, err := d.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	s := &airlock.SharedState{Resource: r.Resource, Config: r.Config, Time: now, Revision: 2, Records: map[string]*airlock.DriverRecord{r.ID: {Request: r, Status: airlock.Cancelled, Finished: now}}}
	m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 0))
	m.ExpectRollback()
	if err := d.persist(context.Background(), tx, s, nil); err == nil {
		t.Fatal("partial upsert accepted")
	}
}

// A single transition removes a batch of retained terminal outcomes, then
// continues to admit work in the same transaction.
func TestIntegrationBatchHistoryCollection(t *testing.T) {
	db := integrationDB(t)
	d := integrationDriver(t, db, uniqueNamespace())
	ctx := context.Background()
	r := request(time.Now())
	r.Config.MaxRecords = 1024
	grant := executeStatus(t, d, airlock.Submit, r, "", airlock.Granted)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	state, before, err := d.load(ctx, tx, r)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 513; i++ {
		old := airlock.NewRequest(state.Time.Add(-time.Second))
		old.Resource = r.Resource
		old.Weight = 1
		old.WaitUntil = old.SubmitUntil
		state.Records[old.ID] = &airlock.DriverRecord{Request: old, Status: airlock.Cancelled, Finished: state.Time.Add(-r.Config.Retention - time.Second)}
	}
	if err = d.persist(ctx, tx, state, before); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	executeStatus(t, d, airlock.Renew, r, grant.LeaseID, airlock.Granted)
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM "+d.requestTable+" WHERE namespace=$1 AND resource=$2", d.namespace, r.Resource).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("batch collection retained %d records", count)
	}
	executeStatus(t, d, airlock.Release, r, grant.LeaseID, airlock.Released)
}

func TestIntegrationBatchExpiryReusesQueuePosition(t *testing.T) {
	db := integrationDB(t)
	d := integrationDriver(t, db, uniqueNamespace())
	ctx := context.Background()
	r := request(time.Now())
	r.Config.MaxRecords = 1024
	r.Config.Admission.MaxQueued = 1024
	r.Config.Admission.MaxQueuedPerTenant = 1024
	r.Config.LeaseTTL = time.Minute
	executeStatus(t, d, airlock.Submit, r, "", airlock.Granted)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	state, before, err := d.load(ctx, tx, r)
	if err != nil {
		t.Fatal(err)
	}
	until := state.Time.Add(time.Millisecond)
	state.Rotations = [][]string{{"batch"}}
	state.Queues = map[string][]string{"batch": {}}
	for i := 0; i < 513; i++ {
		pending := airlock.NewRequest(r.SubmitUntil)
		pending.Resource = r.Resource
		pending.Tenant = "batch"
		pending.Weight = 1
		pending.WaitUntil = until
		state.Records[pending.ID] = &airlock.DriverRecord{Request: pending, Status: airlock.Queued, Until: until}
		state.Queues["batch"] = append(state.Queues["batch"], pending.ID)
	}
	if err = d.persist(ctx, tx, state, before); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Bound the test using backend time, without relying on client/VM clock offsets.
	for {
		var now time.Time
		if err = db.QueryRow("SELECT clock_timestamp()").Scan(&now); err != nil {
			t.Fatal(err)
		}
		if !now.Before(until) {
			break
		}
	}
	replacement := request(time.Now())
	replacement.Config = r.Config
	replacement.Tenant = "batch"
	executeStatus(t, d, airlock.Submit, replacement, "", airlock.Queued)
	var position, count int
	if err = db.QueryRow("SELECT queue_position FROM "+d.requestTable+" WHERE namespace=$1 AND resource=$2 AND id=$3", d.namespace, r.Resource, replacement.ID).Scan(&position); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT COUNT(*) FROM "+d.requestTable+" WHERE namespace=$1 AND resource=$2 AND status='expired'", d.namespace, r.Resource).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if position != 0 || count != 513 {
		t.Fatalf("position=%d expired=%d", position, count)
	}
}

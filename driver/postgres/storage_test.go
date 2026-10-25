package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/tr1v3r/airlock"
)

func TestLoadRelationalFailures(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	for _, mode := range []string{"query", "scan", "rows", "status-position", "position-negative", "queued", "initial-record", "initial-config", "bad-revision", "bad-rotations"} {
		t.Run(mode, func(t *testing.T) {
			d, m := mockDriver(t)
			m.ExpectBegin()
			tx, e := d.db.Begin()
			if e != nil {
				t.Fatal(e)
			}
			defer func() { _ = tx.Rollback() }()
			m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
			config, _ := json.Marshal(r.Config)
			revision, rotations := "1", `[["tenant"]]`
			if mode == "bad-revision" {
				revision = "invalid"
			}
			if mode == "bad-rotations" {
				rotations = "invalid"
			}
			if mode == "initial-record" || mode == "initial-config" {
				revision = "0"
				rotations = "[]"
			}
			if mode == "initial-config" {
				config = []byte(`{}`)
			}
			var observed any = now.UnixMilli()
			if revision == "0" {
				observed = nil
			}
			m.ExpectQuery("SELECT state_version").WillReturnRows(sqlmock.NewRows([]string{"v", "c", "r", "p", "cursor", "rev", "time"}).AddRow(1, config, rotations, "", 0, revision, observed))
			if mode != "bad-revision" && mode != "bad-rotations" {
				q := m.ExpectQuery("SELECT id")
				if mode == "query" {
					q.WillReturnError(errors.New("read failed"))
				} else {
					rows := sqlmock.NewRows([]string{"id", "owner", "tenant", "weight", "submit", "wait", "status", "lease", "until", "finished", "failure", "position"})
					var position any = int64(0)
					if mode == "status-position" {
						position = nil
					}
					if mode == "position-negative" {
						position = int64(-1)
					}
					var weight any = 1
					if mode == "scan" {
						weight = "bad"
					}
					if mode != "initial-config" {
						rows.AddRow(r.ID, r.Owner, r.Tenant, weight, r.SubmitUntil.UnixMilli(), r.WaitUntil.UnixMilli(), "queued", "", r.WaitUntil.UnixMilli(), nil, "", position)
					}
					if mode == "rows" {
						rows.RowError(0, errors.New("stream failed"))
					}
					q.WillReturnRows(rows)
				}
			}
			m.ExpectRollback()
			s, _, err := d.load(context.Background(), tx, r)
			if mode == "queued" {
				if err != nil || len(s.Queues[r.Tenant]) != 1 {
					t.Fatal(s, err)
				}
			} else if err == nil {
				t.Fatal("invalid storage accepted")
			}
		})
	}
}

func TestPersistDeltasAndFailures(t *testing.T) {
	for _, failure := range []string{"", "delete", "insert"} {
		t.Run(failure, func(t *testing.T) {
			d, m := mockDriver(t)
			m.ExpectBegin()
			tx, err := d.db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			now := time.Now().UTC().Truncate(time.Millisecond)
			r := request(now)
			rec := airlock.DriverRecord{Request: r, Status: airlock.Queued, Until: r.WaitUntil}
			state := &airlock.DriverState{Resource: r.Resource, Time: now, Revision: 2, Records: map[string]*airlock.DriverRecord{r.ID: &rec}, Queues: map[string][]string{r.Tenant: {r.ID}}}
			m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
			del := m.ExpectExec("DELETE")
			if failure == "delete" {
				del.WillReturnError(errors.New("delete failed"))
			} else {
				del.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			if failure != "delete" {
				ins := m.ExpectExec("INSERT")
				if failure == "insert" {
					ins.WillReturnError(errors.New("insert failed"))
				} else {
					ins.WillReturnResult(sqlmock.NewResult(0, 1))
				}
			}
			m.ExpectRollback()
			err = d.persist(context.Background(), tx, state, map[string]row{"expired": {}})
			if (err != nil) != (failure != "") {
				t.Fatal(err)
			}
		})
	}
}

func TestNamedCancelAndRelease(t *testing.T) {
	for _, op := range []airlock.Operation{airlock.Cancel, airlock.Release} {
		t.Run(string(op), func(t *testing.T) {
			d, m := mockDriver(t)
			now := time.Now()
			r := request(now)
			data, grant, err := transitionSnapshot(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
			if err != nil {
				t.Fatal(err)
			}
			beforeTransition(m, data, now)
			m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectCommit()
			if op == airlock.Cancel {
				_, err = d.Cancel(context.Background(), r)
			} else {
				_, err = d.Release(context.Background(), airlock.LeaseRef{Request: r, ID: grant.LeaseID})
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTransitionFailureRollsBack(t *testing.T) {
	d, m := mockDriver(t)
	now := time.Now()
	r := request(now)
	data, _, err := transitionSnapshot(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}
	beforeTransition(m, data, now.Add(-time.Second))
	m.ExpectRollback()
	result, err := d.Observe(context.Background(), r)
	if !errors.Is(err, airlock.ErrCorruptState) || result != (airlock.Result{}) {
		t.Fatal(result, err)
	}
}

func TestQueuePositionExhaustionFailsClosed(t *testing.T) {
	for _, mode := range []string{"overflow", "out-of-order"} {
		t.Run(mode, func(t *testing.T) {
			d, m := mockDriver(t)
			m.ExpectBegin()
			tx, err := d.db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectRollback()
			s := &airlock.DriverState{Queues: map[string][]string{"tenant": {"a", "b"}}}
			before := map[string]row{"a": {position: sql.NullInt64{Int64: math.MaxInt64, Valid: true}}}
			if mode == "out-of-order" {
				before["b"] = row{position: sql.NullInt64{Int64: 1, Valid: true}}
			}
			if err = d.persist(context.Background(), tx, s, before); !errors.Is(err, airlock.ErrCorruptState) {
				t.Fatal(err)
			}
		})
	}
}

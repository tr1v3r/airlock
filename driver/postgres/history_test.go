package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/tr1v3r/airlock"
)

func TestReclaimHistoryBoundaries(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, mode := range []string{"no-omitted", "retained", "empty", "delete-error", "affected-error", "over-delete", "query-error", "missing-index", "unexpected-index", "due-index", "rollback-clock"} {
		t.Run(mode, func(t *testing.T) {
			d, m := mockDriver(t)
			m.ExpectBegin()
			tx, err := d.db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			s := &airlock.SharedState{Resource: "r", Records: map[string]*airlock.DriverRecord{}, Time: now}
			stored := storedHistory{total: 3}
			if mode == "no-omitted" {
				stored.total = 0
			}
			wantErr := false
			if mode != "no-omitted" {
				deleted := int64(2)
				if mode == "empty" {
					deleted = 3
				}
				if mode == "over-delete" {
					deleted = 4
				}
				q := m.ExpectExec("DELETE.*gc_after_ms").WithArgs(d.namespace, "r", now.UnixMilli(), "target")
				switch mode {
				case "delete-error":
					q.WillReturnError(errors.New("delete"))
					wantErr = true
				case "affected-error":
					q.WillReturnResult(sqlmock.NewErrorResult(errors.New("affected")))
					wantErr = true
				default:
					q.WillReturnResult(sqlmock.NewResult(0, deleted))
				}
				if mode == "over-delete" {
					wantErr = true
				}
				if !wantErr {
					q := m.ExpectQuery("SELECT gc_after_ms").WithArgs(d.namespace, "r", "target")
					switch mode {
					case "empty", "missing-index":
						q.WillReturnError(sql.ErrNoRows)
					case "query-error":
						q.WillReturnError(errors.New("query"))
					case "due-index":
						q.WillReturnRows(sqlmock.NewRows([]string{"gc"}).AddRow(now.UnixMilli()))
					default:
						q.WillReturnRows(sqlmock.NewRows([]string{"gc"}).AddRow(now.Add(time.Second).UnixMilli()))
					}
					wantErr = mode == "missing-index" || mode == "query-error" || mode == "due-index"
					if mode == "unexpected-index" {
						stored.total = 2
						wantErr = true
					}
				}
			}
			m.ExpectRollback()
			clock := now
			if mode == "rollback-clock" {
				clock = now.Add(-time.Second)
			}
			got, err := d.reclaim(context.Background(), tx, s, "target", clock, stored)
			if (err != nil) != wantErr {
				t.Fatalf("%+v %v", got, err)
			}
			if !wantErr && mode != "no-omitted" && mode != "empty" && got.Count != 1 {
				t.Fatal(got)
			}
		})
	}
}

func TestActiveHistoryLoad(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	state, _, _, err := airlock.Transition(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := json.Marshal(state)
	config, _ := json.Marshal(state.Config)
	rotations, _ := json.Marshal(state.Rotations)
	for _, total := range []any{nil, int64(-1), int64(65), int64(2)} {
		t.Run("count", func(t *testing.T) {
			d, m := mockDriver(t)
			m.ExpectBegin()
			tx, _ := d.db.Begin()
			defer func() { _ = tx.Rollback() }()
			m.ExpectQuery("SELECT state_version").WillReturnRows(sqlmock.NewRows([]string{"version", "config", "rotations", "protected", "cursor", "revision", "time", "capacity", "cv", "total"}).AddRow(1, config, rotations, state.Protected, state.Cursor, "1", now.UnixMilli(), 1, "1", total))
			valid := total == int64(2)
			if valid {
				var full airlock.SharedState
				if err := json.Unmarshal(snapshot, &full); err != nil {
					t.Fatal(err)
				}
				rec := full.Records[r.ID]
				rows := sqlmock.NewRows([]string{"id", "owner", "tenant", "weight", "submit", "wait", "status", "lease", "until", "finished", "failure", "position", "gc"}).AddRow(r.ID, r.Owner, r.Tenant, r.Weight, r.SubmitUntil.UnixMilli(), r.WaitUntil.UnixMilli(), rec.Status, rec.LeaseID, millis(rec.Until), nil, "", nil, nil)
				m.ExpectQuery("SELECT id.*UNION ALL SELECT").WithArgs(d.namespace, r.Resource, r.ID).WillReturnRows(rows)
			}
			m.ExpectRollback()
			var h storedHistory
			got, before, err := d.loadState(context.Background(), tx, r.Resource, nil, r.ID, &h)
			if !valid {
				if !errors.Is(err, airlock.ErrCorruptState) {
					t.Fatal(err)
				}
				return
			}
			if err != nil || got.Version != 1 || h.total != 2 || len(before) != 1 {
				t.Fatalf("%+v %+v %v", got, h, err)
			}
		})
	}
}

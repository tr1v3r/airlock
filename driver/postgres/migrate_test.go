package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/tr1v3r/airlock"
)

func TestMigrationBoundaries(t *testing.T) {
	for _, fault := range []string{"", "begin", "lock", "check", "check-close", "alter", "create", "read", "read-rows", "scan", "close", "json", "state", "config", "persist", "finish", "commit"} {
		t.Run(fault, func(t *testing.T) {
			d, m := mockDriver(t)
			sentinel := errors.New("migration failure")
			now := time.Now()
			r := request(now)
			data, _, err := airlock.Transition(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
			if err != nil {
				t.Fatal(err)
			}
			// Build only the expected prefix; every failure must roll the transaction back.
			stopped := false
			exec := func(name, query string) {
				if stopped {
					return
				}
				e := m.ExpectExec(query)
				if fault == name {
					e.WillReturnError(sentinel)
					stopped = true
				} else {
					e.WillReturnResult(sqlmock.NewResult(0, 1))
				}
			}
			begin := m.ExpectBegin()
			if fault == "begin" {
				begin.WillReturnError(sentinel)
				stopped = true
			}
			exec("lock", "LOCK TABLE")
			if !stopped {
				q := m.ExpectQuery("SELECT snapshot")
				rows := sqlmock.NewRows([]string{"snapshot"})
				switch fault {
				case "check":
					q.WillReturnError(sentinel)
					stopped = true
				case "check-close":
					q.WillReturnRows(rows.CloseError(sentinel))
					stopped = true
				default:
					q.WillReturnRows(rows)
				}
			}
			exec("alter", "ALTER TABLE")
			exec("create", "CREATE TABLE")
			if !stopped {
				q := m.ExpectQuery("SELECT namespace")
				rows := sqlmock.NewRows([]string{"namespace", "resource", "snapshot"})
				switch fault {
				case "read":
					q.WillReturnError(sentinel)
					stopped = true
				case "read-rows":
					q.WillReturnRows(rows.AddRow("test", r.Resource, data).RowError(0, sentinel))
					stopped = true
				case "scan":
					q.WillReturnRows(rows.AddRow(nil, r.Resource, data))
					stopped = true
				case "close":
					q.WillReturnRows(rows.AddRow("test", r.Resource, data).CloseError(sentinel))
					stopped = true
				case "json":
					q.WillReturnRows(rows.AddRow("test", r.Resource, []byte("bad")))
					stopped = true
				case "state":
					var s airlock.DriverState
					_ = json.Unmarshal(data, &s)
					s.Version = 99
					bad, _ := json.Marshal(s)
					q.WillReturnRows(rows.AddRow("test", r.Resource, bad))
					stopped = true
				default:
					q.WillReturnRows(rows.AddRow("test", r.Resource, data))
				}
			}
			exec("config", "UPDATE .*SET config")
			exec("persist", "UPDATE .*SET rotations")
			exec("insert", "INSERT INTO")
			if !stopped {
				m.ExpectQuery("SELECT namespace").WillReturnRows(sqlmock.NewRows([]string{"namespace", "resource", "snapshot"}))
			}
			exec("finish", "ALTER TABLE")
			if !stopped {
				e := m.ExpectCommit()
				if fault == "commit" {
					e.WillReturnError(sentinel)
				}
			} else if fault != "begin" {
				m.ExpectRollback()
			}
			err = d.MigrateSnapshotSchema(context.Background())
			if (err != nil) != (fault != "") {
				t.Fatal(err)
			}
		})
	}
	var d *Driver
	if err := d.MigrateSnapshotSchema(context.Background()); err == nil {
		t.Fatal("nil driver")
	}
}

func TestSchemaFailures(t *testing.T) {
	for _, fault := range []string{"begin", "query", "close", "commit"} {
		t.Run(fault, func(t *testing.T) {
			d, m := mockDriver(t)
			sentinel := errors.New("schema failed")
			b := m.ExpectBegin()
			if fault == "begin" {
				b.WillReturnError(sentinel)
			} else {
				for i := 0; i < 2; i++ {
					m.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
				}
				q := m.ExpectQuery("SELECT state_version")
				switch fault {
				case "query":
					q.WillReturnError(sentinel)
					m.ExpectRollback()
				case "close":
					q.WillReturnRows(sqlmock.NewRows([]string{"x"}).CloseError(sentinel))
					m.ExpectRollback()
				default:
					q.WillReturnRows(sqlmock.NewRows([]string{"x"}))
					m.ExpectQuery("SELECT id").WillReturnRows(sqlmock.NewRows([]string{"x"}))
					m.ExpectCommit().WillReturnError(sentinel)
				}
			}
			if err := d.EnsureSchema(context.Background()); !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
		})
	}
}

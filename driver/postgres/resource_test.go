package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/tr1v3r/airlock"
)

func TestInspectResource(t *testing.T) {
	for _, mode := range []string{"ok", "missing", "query", "scan", "version", "revision", "uninitialized", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			d, m := mockDriver(t)
			now := time.Now().UTC().Truncate(time.Millisecond)
			values := []any{1, "1", "2", now.UnixMilli(), 2, 0, now}
			if mode == "scan" {
				values[0] = "bad"
			}
			if mode == "version" {
				values[1] = "bad"
			}
			if mode == "revision" {
				values[2] = "bad"
			}
			if mode == "uninitialized" {
				values[2] = "0"
			}
			if mode == "corrupt" {
				values[0] = -1
			}
			rows := sqlmock.NewRows([]string{"capacity", "version", "revision", "time", "used", "queued", "now"})
			if mode != "missing" {
				rows.AddRow(values[0], values[1], values[2], values[3], values[4], values[5], values[6])
			}
			expect := m.ExpectQuery("SELECT r.capacity")
			if mode == "query" {
				expect.WillReturnError(errors.New("read failed"))
			} else {
				expect.WillReturnRows(rows)
			}
			status, err := d.InspectResource(context.Background(), "r")
			if mode == "ok" {
				if err != nil || status.Capacity != 1 || status.Used != 2 || status.Revision != 2 {
					t.Fatal(status, err)
				}
			} else if err == nil || status != (airlock.ResourceStatus{}) {
				t.Fatal(status, err)
			}
		})
	}
	d, _ := mockDriver(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.InspectResource(ctx, "r"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := d.InspectResource(context.Background(), ""); !errors.Is(err, airlock.ErrInvalidRequest) {
		t.Fatal(err)
	}
}

func TestResizeTransaction(t *testing.T) {
	for _, mode := range []string{"commit", "conflict", "missing", "commit-error"} {
		t.Run(mode, func(t *testing.T) {
			d, m := mockDriver(t)
			now := time.Now().UTC().Truncate(time.Millisecond)
			r := request(now)
			snapshot, _, err := transitionSnapshot(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
			if err != nil {
				t.Fatal(err)
			}
			m.ExpectBegin()
			x := m.ExpectQuery("SELECT state_version")
			if mode == "missing" {
				x.WillReturnError(sql.ErrNoRows)
			} else {
				x.WillReturnRows(resourceRows(snapshot, now))
				m.ExpectQuery("SELECT id").WillReturnRows(requestRows(snapshot))
				m.ExpectQuery("SELECT pg_catalog.clock_timestamp").WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(now))
			}
			if mode == "commit" || mode == "commit-error" {
				m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
				if mode == "commit-error" {
					m.ExpectCommit().WillReturnError(errors.New("lost commit"))
				} else {
					m.ExpectCommit()
				}
			} else {
				m.ExpectRollback()
			}
			version := uint64(1)
			if mode == "conflict" {
				version = 2
			}
			result, err := d.Resize(context.Background(), airlock.ResizeRequest{Resource: r.Resource, Capacity: 2, ExpectedVersion: version})
			if mode == "commit" {
				if err != nil || result.Capacity != 2 || result.CapacityVersion != 2 {
					t.Fatal(result, err)
				}
			} else if err == nil || result != (airlock.ResourceStatus{}) {
				t.Fatal(result, err)
			}
		})
	}
}

func TestIntegrationResizeRestartAndReadOnlyInspect(t *testing.T) {
	db := integrationDB(t)
	namespace := uniqueNamespace()
	d := integrationDriver(t, db, namespace)
	ctx := context.Background()
	r := request(time.Now())
	if _, err := d.Submit(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Resize(ctx, airlock.ResizeRequest{Resource: r.Resource, Capacity: 2, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(db, namespace)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SELECT resource FROM %s WHERE namespace=$1 AND resource=$2 FOR UPDATE", d.resourceTable), namespace, r.Resource); err != nil {
		t.Fatal(err)
	}
	read, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	status, err := restarted.InspectResource(read, r.Resource)
	if err != nil || status.Capacity != 2 || status.CapacityVersion != 2 {
		t.Fatal(status, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	isolated, err := New(db, namespace+"-other")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := isolated.InspectResource(ctx, r.Resource); !errors.Is(err, airlock.ErrResourceNotFound) {
		t.Fatal(err)
	}
}

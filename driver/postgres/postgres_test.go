package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/tr1v3r/airlock"
)

func mockDriver(t *testing.T, options ...Option) (*Driver, sqlmock.Sqlmock) {
	t.Helper()
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	d, err := New(db, "test", options...)
	if err != nil {
		t.Fatal(err)
	}
	return d, m
}
func request(now time.Time) airlock.Request {
	now = now.UTC().Truncate(time.Millisecond)
	r := airlock.NewRequest(now.Add(20 * time.Second))
	r.Resource, r.Tenant, r.Weight = "test-resource", "tenant", 1
	r.WaitUntil = now.Add(20 * time.Second)
	r.Config = airlock.ResourceConfig{Admission: airlock.Config{Capacity: 1, MaxQueued: 8, MaxQueuedPerTenant: 8}, LeaseTTL: time.Second, MaxWait: 30 * time.Second, SubmitWindow: 30 * time.Second, Retention: time.Second, MaxRecords: 64}
	return r
}
func beforeTransition(m sqlmock.Sqlmock, snapshot []byte, now time.Time) {
	m.ExpectBegin()
	m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectQuery("SELECT snapshot").WillReturnRows(sqlmock.NewRows([]string{"snapshot"}).AddRow(snapshot))
	m.ExpectQuery("SELECT pg_catalog.clock_timestamp").WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(now))
}

func TestConfiguration(t *testing.T) {
	if _, err := New(nil, ""); err == nil {
		t.Fatal("nil db accepted")
	}
	db := new(sql.DB)
	for _, prefix := range []string{"\x00", "\xff", strings.Repeat("a", 257)} {
		if _, err := New(db, prefix); err == nil {
			t.Fatal("invalid prefix accepted")
		}
	}
	for _, options := range [][]Option{{nil}, {WithOperationTimeout(0)}, {WithOperationTimeout(-time.Second)}} {
		if _, err := New(db, "", options...); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	for _, name := range []string{"", "a.b.c", ".a", "a.", "1name", "a;DROP", "hé", strings.Repeat("a", 64)} {
		if _, err := New(db, "", WithTable(name)); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	for name, want := range map[string]string{"abc_1": `"public"."abc_1"`, "MySchema.MyTable": `"MySchema"."MyTable"`} {
		d, err := New(db, "", WithTable(name), WithOperationTimeout(time.Minute))
		if err != nil || d.table != want || d.timeout != time.Minute {
			t.Fatalf("%+v %v", d, err)
		}
	}
}

func TestValidationWithoutDatabase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, d := range []*Driver{nil, {}, {db: new(sql.DB)}} {
		if err := d.EnsureSchema(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, err := d.Execute(ctx, airlock.Command{}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	for _, d := range []*Driver{nil, {}} {
		if err := d.EnsureSchema(context.Background()); err == nil {
			t.Fatal("invalid driver")
		}
		if _, err := d.Execute(context.Background(), airlock.Command{}); err == nil {
			t.Fatal("invalid driver")
		}
	}
	d, _ := mockDriver(t)
	if _, err := d.Execute(context.Background(), airlock.Command{}); !errors.Is(err, airlock.ErrInvalidConfig) {
		t.Fatal(err)
	}
}

func TestEnsureSchema(t *testing.T) {
	d, m := mockDriver(t)
	m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := d.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("permission denied")
	m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnError(sentinel)
	if err := d.EnsureSchema(context.Background()); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}

func TestExecuteFailureBoundaries(t *testing.T) {
	now := time.Now()
	for _, stage := range []string{"begin", "insert", "lock", "clock", "corrupt", "update", "count", "missing", "commit"} {
		t.Run(stage, func(t *testing.T) {
			d, m := mockDriver(t)
			sentinel := errors.New("connection failed")
			switch stage {
			case "begin":
				m.ExpectBegin().WillReturnError(sentinel)
			case "insert":
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO").WillReturnError(sentinel)
			case "lock":
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
				m.ExpectQuery("SELECT snapshot").WillReturnError(sentinel)
			case "clock":
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
				m.ExpectQuery("SELECT snapshot").WillReturnRows(sqlmock.NewRows([]string{"snapshot"}).AddRow([]byte{}))
				m.ExpectQuery("SELECT pg_catalog.clock_timestamp").WillReturnError(sentinel)
			case "corrupt":
				beforeTransition(m, []byte("corrupt"), now)
			default:
				beforeTransition(m, nil, now)
				switch stage {
				case "update":
					m.ExpectExec("UPDATE").WillReturnError(sentinel)
				case "count":
					m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewErrorResult(sentinel))
				case "missing":
					m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
				case "commit":
					m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
					m.ExpectCommit().WillReturnError(sentinel)
				}
			}
			if stage != "begin" && stage != "commit" {
				m.ExpectRollback()
			}
			got, err := d.Execute(context.Background(), airlock.Command{Kind: airlock.Submit, Request: request(now)})
			if err == nil || got != (airlock.Result{}) {
				t.Fatalf("unsafe result=%+v err=%v", got, err)
			}
		})
	}
}

func TestBusinessErrorCommitsMaintenance(t *testing.T) {
	d, m := mockDriver(t)
	now := time.Now()
	r := request(now)
	initial, _, err := airlock.Transition(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}
	beforeTransition(m, initial, now.Add(2*time.Second))
	m.ExpectExec("UPDATE").WithArgs("test", r.Resource, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	got, err := d.Execute(context.Background(), airlock.Command{Kind: airlock.Renew, Request: r, LeaseID: "stale"})
	if !errors.Is(err, airlock.ErrLeaseLost) || got.Status == airlock.Granted {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestAmbiguousCommitRecovery(t *testing.T) {
	d, m := mockDriver(t)
	now := time.Now()
	r := request(now)
	cmd := airlock.Command{Kind: airlock.Submit, Request: r}
	// Model a server-side commit whose response is lost. Transition is deterministic
	// for a request, so the separately computed durable snapshot is authoritative.
	durable, granted, err := airlock.Transition(nil, now, cmd)
	if err != nil {
		t.Fatal(err)
	}
	beforeTransition(m, nil, now)
	m.ExpectExec("UPDATE").WithArgs("test", r.Resource, durable).WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit().WillReturnError(errors.New("commit response lost"))
	if got, err := d.Execute(context.Background(), cmd); err == nil || got != (airlock.Result{}) {
		t.Fatalf("%+v %v", got, err)
	}
	beforeTransition(m, durable, now.Add(time.Millisecond))
	m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	cmd.Kind = airlock.Observe
	got, err := d.Execute(context.Background(), cmd)
	if err != nil || got.Status != airlock.Granted || got.LeaseID != granted.LeaseID || got.Remaining >= granted.Remaining {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestOperationDeadline(t *testing.T) {
	d, m := mockDriver(t, WithOperationTimeout(time.Millisecond))
	m.ExpectBegin().WillDelayFor(20 * time.Millisecond)
	started := time.Now()
	if got, err := d.Execute(context.Background(), airlock.Command{Kind: airlock.Submit, Request: request(started)}); err == nil || got != (airlock.Result{}) {
		t.Fatalf("%+v %v", got, err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("operation was not bounded")
	}
}

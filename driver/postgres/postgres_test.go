package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/tr1v3r/airlock/protocol"
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
func request(now time.Time) protocol.Request {
	now = now.UTC().Truncate(time.Millisecond)
	r := protocol.NewRequest(now.Add(20 * time.Second))
	r.Resource, r.Tenant, r.Weight = "test-resource", "tenant", 1
	r.WaitUntil = now.Add(20 * time.Second)
	r.Config = protocol.ResourceConfig{Admission: protocol.Config{Capacity: 1, MaxQueued: 8, MaxQueuedPerTenant: 8}, LeaseTTL: time.Second, MaxWait: 30 * time.Second, SubmitWindow: 30 * time.Second, Retention: time.Second, MaxRecords: 64}
	return r
}
func resourceRows(snapshot []byte, now time.Time) *sqlmock.Rows {
	s := protocol.State{Version: 1, Config: request(now).Config, Rotations: [][]string{}}
	if len(snapshot) > 0 {
		_ = json.Unmarshal(snapshot, &s)
	}
	config, _ := json.Marshal(s.Config)
	rotations, _ := json.Marshal(s.Rotations)
	if string(snapshot) == "corrupt" {
		config = []byte("corrupt")
	}
	return sqlmock.NewRows(strings.Split(strings.ReplaceAll(resourceColumns, " ", ""), ",")).AddRow(s.Version, config, rotations, s.Protected, s.Cursor, strconv.FormatUint(s.Revision, 10), millis(s.Time))
}
func requestRows(snapshot []byte) *sqlmock.Rows {
	rows := sqlmock.NewRows(strings.Split(strings.ReplaceAll(requestColumns, " ", ""), ","))
	var s protocol.State
	_ = json.Unmarshal(snapshot, &s)
	for _, r := range s.Records {
		rows.AddRow(r.Request.ID, r.Request.Owner, r.Request.Tenant, r.Request.Weight, r.Request.SubmitUntil.UnixMilli(), r.Request.WaitUntil.UnixMilli(), string(r.Status), r.LeaseID, millis(r.Until), millis(r.Finished), r.Failure, nil)
	}
	return rows
}
func beforeTransition(m sqlmock.Sqlmock, snapshot []byte, now time.Time) {
	m.ExpectBegin()
	m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectQuery("SELECT state_version").WillReturnRows(resourceRows(snapshot, now))
	if string(snapshot) == "corrupt" {
		return
	}
	m.ExpectQuery("SELECT id").WillReturnRows(requestRows(snapshot))
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
		if _, err := d.Submit(ctx, protocol.Request{}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	for _, d := range []*Driver{nil, {}} {
		if err := d.EnsureSchema(context.Background()); err == nil {
			t.Fatal("invalid driver")
		}
		if _, err := d.Submit(context.Background(), protocol.Request{}); err == nil {
			t.Fatal("invalid driver")
		}
	}
	d, _ := mockDriver(t)
	if _, err := d.Submit(context.Background(), protocol.Request{}); !errors.Is(err, protocol.ErrInvalidConfig) {
		t.Fatal(err)
	}
}

func TestEnsureSchema(t *testing.T) {
	d, m := mockDriver(t)
	m.ExpectBegin()
	for i := 0; i < 2; i++ {
		m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	}
	m.ExpectQuery("SELECT state_version").WillReturnRows(sqlmock.NewRows([]string{"unused"}))
	m.ExpectQuery("SELECT id").WillReturnRows(sqlmock.NewRows([]string{"unused"}))
	m.ExpectCommit()
	if err := d.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("permission denied")
	m.ExpectBegin()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnError(sentinel)
	m.ExpectRollback()
	if err := d.EnsureSchema(context.Background()); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	// Begin failures surface with schema context.
	m.ExpectBegin().WillReturnError(sentinel)
	if err := d.EnsureSchema(context.Background()); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	// Missing required columns fail the schema probe.
	m.ExpectBegin()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	m.ExpectQuery("SELECT state_version").WillReturnError(sentinel)
	m.ExpectRollback()
	err := d.EnsureSchema(context.Background())
	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "incompatible schema") {
		t.Fatal(err)
	}
	// Row-close failures abort the probe transaction.
	m.ExpectBegin()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	m.ExpectQuery("SELECT state_version").WillReturnRows(sqlmock.NewRows([]string{"unused"}).CloseError(sentinel))
	m.ExpectRollback()
	if err = d.EnsureSchema(context.Background()); !errors.Is(err, sentinel) {
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
				m.ExpectQuery("SELECT state_version").WillReturnError(sentinel)
			case "clock":
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
				m.ExpectQuery("SELECT state_version").WillReturnRows(resourceRows(nil, now))
				m.ExpectQuery("SELECT id").WillReturnRows(requestRows(nil))
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
					m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
					m.ExpectCommit().WillReturnError(sentinel)
				}
			}
			if stage != "begin" && stage != "commit" {
				m.ExpectRollback()
			}
			got, err := d.Submit(context.Background(), request(now))
			if err == nil || got != (protocol.Result{}) {
				t.Fatalf("unsafe result=%+v err=%v", got, err)
			}
		})
	}
}

func TestBusinessErrorCommitsMaintenance(t *testing.T) {
	d, m := mockDriver(t)
	now := time.Now()
	r := request(now)
	initial, _, err := protocol.ApplySnapshot(nil, now, protocol.Command{Kind: protocol.Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}
	beforeTransition(m, initial, now.Add(2*time.Second))
	m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	got, err := d.Renew(context.Background(), protocol.LeaseRef{Request: r, ID: "stale"})
	if !errors.Is(err, protocol.ErrLeaseLost) || got.Status == protocol.Granted {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestAmbiguousCommitRecovery(t *testing.T) {
	d, m := mockDriver(t)
	now := time.Now()
	r := request(now)
	cmd := protocol.Command{Kind: protocol.Submit, Request: r}
	// Model a server-side commit whose response is lost. Transition is deterministic
	// for a request, so the separately computed durable snapshot is authoritative.
	durable, granted, err := protocol.ApplySnapshot(nil, now, cmd)
	if err != nil {
		t.Fatal(err)
	}
	beforeTransition(m, nil, now)
	m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit().WillReturnError(errors.New("commit response lost"))
	if got, err := d.Submit(context.Background(), r); err == nil || got != (protocol.Result{}) {
		t.Fatalf("%+v %v", got, err)
	}
	beforeTransition(m, durable, now.Add(time.Millisecond))
	m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	got, err := d.Observe(context.Background(), r)
	if err != nil || got.Status != protocol.Granted || got.LeaseID != granted.LeaseID || got.Remaining >= granted.Remaining {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestOperationDeadline(t *testing.T) {
	d, m := mockDriver(t, WithOperationTimeout(time.Millisecond))
	m.ExpectBegin().WillDelayFor(20 * time.Millisecond)
	started := time.Now()
	if got, err := d.Submit(context.Background(), request(started)); err == nil || got != (protocol.Result{}) {
		t.Fatalf("%+v %v", got, err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("operation was not bounded")
	}
}

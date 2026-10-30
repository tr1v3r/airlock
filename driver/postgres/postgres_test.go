package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/internal/driverconformance"
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

func resourceRows(snapshot []byte, now time.Time) *sqlmock.Rows {
	s := airlock.SharedState{Version: 1, Config: request(now).Config, Capacity: request(now).Config.Admission.Capacity, CapacityVersion: 1, Rotations: [][]string{}}
	if len(snapshot) > 0 {
		_ = json.Unmarshal(snapshot, &s)
	}

	config, _ := json.Marshal(s.Config)
	rotations, _ := json.Marshal(s.Rotations)
	if string(snapshot) == "corrupt" {
		config = []byte("corrupt")
	}
	return sqlmock.NewRows(strings.Split(strings.ReplaceAll(resourceColumns, " ", ""), ",")).AddRow(s.Version, config, rotations, s.Protected, s.Cursor, strconv.FormatUint(s.Revision, 10), millis(s.Time), s.Capacity, strconv.FormatUint(s.CapacityVersion, 10), nil)
}

func requestRows(snapshot []byte) *sqlmock.Rows {
	rows := sqlmock.NewRows(strings.Split(strings.ReplaceAll(requestColumns, " ", ""), ","))
	var s airlock.SharedState
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

	for _, namespace := range []string{"\x00", "\xff", strings.Repeat("a", 257)} {
		if _, err := New(db, namespace); err == nil {
			t.Fatal("invalid namespace accepted")
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

	for _, tc := range []struct {
		name, resources, requests string
	}{
		{"abc_1", `"public"."abc_1"`, `"public"."abc_1_requests"`},
		{"MySchema.MyTable", `"MySchema"."MyTable"`, `"MySchema"."MyTable_requests"`},
		{"airlock_resources", `"public"."airlock_resources"`, `"public"."airlock_requests"`},
		{"public.airlock_resources", `"public"."airlock_resources"`, `"public"."airlock_requests"`},
		{"app.jobs_resources", `"app"."jobs_resources"`, `"app"."jobs_resources_requests"`},
	} {
		d, err := New(db, "", WithTable(tc.name), WithOperationTimeout(time.Minute))
		if err != nil || d.resourceTable != tc.resources || d.requestTable != tc.requests || d.timeout != time.Minute {
			t.Fatalf("%s: %+v %v", tc.name, d, err)
		}
	}

	d, err := New(db, "")
	if err != nil || d.resourceTable != `"public"."airlock_resources"` || d.requestTable != `"public"."airlock_requests"` {
		t.Fatalf("default tables: %+v %v", d, err)
	}
}

func TestValidationWithoutDatabase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, d := range []*Driver{nil, {}, {db: new(sql.DB)}} {
		if err := d.EnsureSchema(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, err := d.Submit(ctx, airlock.Request{}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}

	for _, d := range []*Driver{nil, {}} {
		if err := d.EnsureSchema(context.Background()); err == nil {
			t.Fatal("invalid driver")
		}
		if _, err := d.Submit(context.Background(), airlock.Request{}); err == nil {
			t.Fatal("invalid driver")
		}
	}

	d, _ := mockDriver(t)
	if _, err := d.Submit(context.Background(), airlock.Request{}); !errors.Is(err, airlock.ErrInvalidConfig) {
		t.Fatal(err)
	}
}

func TestEnsureSchema(t *testing.T) {
	d, m := mockDriver(t)
	m.ExpectBegin()

	for _, ddl := range d.schema() {
		m.ExpectExec(regexp.QuoteMeta(ddl)).WillReturnResult(sqlmock.NewResult(0, 0))
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
	for _, ddl := range d.schema() {
		m.ExpectExec(regexp.QuoteMeta(ddl)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	m.ExpectQuery("SELECT state_version").WillReturnError(sentinel)
	m.ExpectRollback()
	err := d.EnsureSchema(context.Background())
	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "incompatible schema") {
		t.Fatal(err)
	}

	// Row-close failures abort the probe transaction.
	m.ExpectBegin()
	for _, ddl := range d.schema() {
		m.ExpectExec(regexp.QuoteMeta(ddl)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
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
	initial, _, err := transitionSnapshot(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}

	beforeTransition(m, initial, now.Add(2*time.Second))
	m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	got, err := d.Renew(context.Background(), airlock.LeaseRef{Request: r, ID: "stale"})
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
	durable, granted, err := transitionSnapshot(nil, now, cmd)
	if err != nil {
		t.Fatal(err)
	}

	beforeTransition(m, nil, now)
	m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit().WillReturnError(errors.New("commit response lost"))
	if got, err := d.Submit(context.Background(), r); err == nil || got != (airlock.Result{}) {
		t.Fatalf("%+v %v", got, err)
	}

	m.ExpectQuery("SELECT r.config").WillReturnRows(observationRows(durable, r, now.Add(time.Millisecond)))
	got, err := d.Observe(context.Background(), r)
	if err != nil || got.Status != airlock.Granted || got.LeaseID != granted.LeaseID || got.Remaining >= granted.Remaining {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestOperationDeadline(t *testing.T) {
	d, m := mockDriver(t, WithOperationTimeout(time.Millisecond))
	m.ExpectBegin().WillDelayFor(20 * time.Millisecond)
	started := time.Now()
	if got, err := d.Submit(context.Background(), request(started)); err == nil || got != (airlock.Result{}) {
		t.Fatalf("%+v %v", got, err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("operation was not bounded")
	}
}

// A structurally valid stored grant must not be overwritten by an identity or
// configuration conflict. Unlike business rejections that perform maintenance,
// these transitions return no next snapshot and the transaction must roll back.
func TestConflictingCommandsRollbackWithoutPersist(t *testing.T) {
	for _, conflict := range []string{"owner", "configuration"} {
		for _, operation := range []airlock.Operation{airlock.Submit, airlock.Observe, airlock.Cancel, airlock.Renew, airlock.Release} {
			t.Run(conflict+"/"+string(operation), func(t *testing.T) {
				d, mock := mockDriver(t)
				now := time.Now().UTC().Truncate(time.Millisecond)
				r := request(now)
				snapshot, grant, err := transitionSnapshot(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
				if err != nil || grant.Status != airlock.Granted {
					t.Fatalf("seed: %+v err=%v", grant, err)
				}

				wrong := r
				want := airlock.ErrRequestConflict
				if conflict == "owner" {
					wrong.Owner = airlock.NewRequest(now).Owner
				} else {
					wrong.Config.Retention += time.Millisecond
					want = airlock.ErrConfigConflict
				}

				if operation == airlock.Observe {
					mock.ExpectQuery("SELECT r.config").WillReturnRows(observationRows(snapshot, r, now.Add(time.Millisecond)))
				} else {
					beforeTransition(mock, snapshot, now.Add(time.Millisecond))
					// No UPDATE or COMMIT expectation: either would fail this test.
					mock.ExpectRollback()
				}

				got, err := driverconformance.Invoke(context.Background(), d, airlock.Command{Kind: operation, Request: wrong, LeaseID: grant.LeaseID})
				if !errors.Is(err, want) || got != (airlock.Result{}) {
					t.Fatalf("conflicting %s exposed result: %+v err=%v want=%v", operation, got, err, want)
				}
			})
		}
	}
}

func TestBackwardBackendClockRollsBackValidSnapshot(t *testing.T) {
	d, mock := mockDriver(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	snapshot, grant, err := transitionSnapshot(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
	if err != nil || grant.Status != airlock.Granted {
		t.Fatalf("seed: %+v err=%v", grant, err)
	}

	beforeTransition(mock, snapshot, now.Add(-time.Millisecond))
	mock.ExpectRollback()
	got, err := d.Renew(context.Background(), airlock.LeaseRef{Request: r, ID: grant.LeaseID})
	if !errors.Is(err, airlock.ErrCorruptState) || got != (airlock.Result{}) {
		t.Fatalf("backward clock returned usable renewal: %+v err=%v", got, err)
	}
}

func transitionSnapshot(data []byte, now time.Time, cmd airlock.Command) ([]byte, airlock.Result, error) {
	state, err := airlock.DecodeState(data)
	if err != nil {
		return nil, airlock.Result{}, err
	}

	next, result, _, err := airlock.Transition(state, now, cmd)
	if next == nil {
		return nil, result, err
	}

	encoded, codecErr := airlock.EncodeState(next)
	if codecErr != nil {
		return nil, airlock.Result{}, codecErr
	}
	return encoded, result, err
}

// observationRows contains only a resource header and the selected request.
func observationRows(snapshot []byte, request airlock.Request, now time.Time) *sqlmock.Rows {
	var s airlock.SharedState
	_ = json.Unmarshal(snapshot, &s)
	config, _ := json.Marshal(s.Config)
	values := []any{config, millis(s.Time), strconv.FormatUint(s.Revision, 10), millis(s.NextMaintenance())}
	rec := s.Records[request.ID]
	if rec == nil {
		for range 11 {
			values = append(values, nil)
		}
	} else {
		values = append(values, rec.Request.ID, rec.Request.Owner, rec.Request.Tenant, rec.Request.Weight,
			rec.Request.SubmitUntil.UnixMilli(), rec.Request.WaitUntil.UnixMilli(), string(rec.Status),
			rec.LeaseID, millis(rec.Until), millis(rec.Finished), rec.Failure)
	}

	values = append(values, now)
	rows := sqlmock.NewRows([]string{"config", "time", "revision", "next", "id", "owner", "tenant", "weight", "submit", "wait", "status", "lease", "until", "finished", "failure", "now"})
	converted := make([]driver.Value, len(values))

	for i, value := range values {
		converted[i] = value
	}

	return rows.AddRow(converted...)
}

func TestObserveProjectionErrors(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	snapshot, _, err := transitionSnapshot(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}

	for _, mode := range []string{"query", "missing", "revision", "config", "initial", "missing-record", "null-field", "backward", "negative-weight"} {
		t.Run(mode, func(t *testing.T) {
			d, m := mockDriver(t)
			e := m.ExpectQuery("SELECT r.config")

			switch mode {
			case "query":
				e.WillReturnError(errors.New("read failed"))
			case "missing":
				e.WillReturnRows(sqlmock.NewRows([]string{"empty"}))
			default:
				// Construct explicit projection rows for malformed headers/records.
				var s airlock.SharedState
				_ = json.Unmarshal(snapshot, &s)
				config, _ := json.Marshal(s.Config)
				rec := s.Records[r.ID]
				values := []driver.Value{config, millis(s.Time), fmt.Sprint(s.Revision), millis(s.NextMaintenance()),
					rec.Request.ID, rec.Request.Owner, rec.Request.Tenant, rec.Request.Weight, rec.Request.SubmitUntil.UnixMilli(),
					rec.Request.WaitUntil.UnixMilli(), string(rec.Status), rec.LeaseID, millis(rec.Until), millis(rec.Finished), rec.Failure, now}

				switch mode {
				case "revision":
					values[2] = "invalid"
				case "negative-weight":
					values[7] = int64(-1)
				case "config":
					values[0] = []byte("{")
				case "initial":
					values[2] = "0"
				case "missing-record":
					for i := 4; i < 15; i++ {
						values[i] = nil
					}
				case "null-field":
					values[5] = nil
				case "backward":
					values[15] = now.Add(-time.Second)
				}

				rows := sqlmock.NewRows([]string{"config", "time", "revision", "next", "id", "owner", "tenant", "weight", "submit", "wait", "status", "lease", "until", "finished", "failure", "now"}).AddRow(values...)
				e.WillReturnRows(rows)
			}

			_, ready, err := d.observe(context.Background(), d.db, r)
			fallback := mode == "missing" || mode == "initial" || mode == "missing-record"
			if fallback {
				if ready || err != nil {
					t.Fatalf("%t %v", ready, err)
				}
			} else if !ready || err == nil {
				t.Fatalf("%t %v", ready, err)
			}
		})
	}
}

func TestObserveMaintenanceRecheck(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	snapshot, _, err := transitionSnapshot(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}

	for _, mode := range []string{"missing", "lock-error", "peer-maintained", "due"} {
		t.Run(mode, func(t *testing.T) {
			d, m := mockDriver(t)
			m.ExpectBegin()
			tx, err := d.db.Begin()
			if err != nil {
				t.Fatal(err)
			}

			lock := m.ExpectQuery("SELECT resource")

			switch mode {
			case "missing":
				lock.WillReturnRows(sqlmock.NewRows([]string{"resource"}))
			case "lock-error":
				lock.WillReturnError(errors.New("lock failed"))
			default:
				lock.WillReturnRows(sqlmock.NewRows([]string{"resource"}).AddRow(r.Resource))
				at := now
				if mode == "due" {
					at = now.Add(r.Config.LeaseTTL)
				}

				m.ExpectQuery("SELECT r.config").WillReturnRows(observationRows(snapshot, r, at))
			}

			m.ExpectRollback()
			_, ready, err := d.observeLocked(context.Background(), tx, r)
			if mode == "lock-error" {
				if err == nil {
					t.Fatal("missing lock failure")
				}
			} else if err != nil || ready != (mode == "peer-maintained") {
				t.Fatalf("%t %v", ready, err)
			}

			_ = tx.Rollback()
		})
	}
}

func TestObserveValidationAndMissingResource(t *testing.T) {
	d, m := mockDriver(t)
	r := request(time.Now())
	bad := r
	bad.Owner = ""
	if _, err := d.Observe(context.Background(), bad); !errors.Is(err, airlock.ErrInvalidRequest) {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Observe(ctx, r); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// Unknown results still follow Transition's submission-window/error rules.
	now := time.Now()
	m.ExpectQuery("SELECT r.config").WillReturnRows(sqlmock.NewRows([]string{"empty"}))
	m.ExpectBegin()
	m.ExpectQuery("SELECT resource").WillReturnRows(sqlmock.NewRows([]string{"resource"}))
	m.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectQuery("SELECT state_version").WillReturnRows(resourceRows(nil, now))
	m.ExpectQuery("SELECT id").WillReturnRows(requestRows(nil))
	m.ExpectQuery("SELECT pg_catalog.clock_timestamp").WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(now))
	m.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	if _, err := d.Observe(context.Background(), r); !errors.Is(err, airlock.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestObservePeerRenewalAvoidsFullLoad(t *testing.T) {
	d, m := mockDriver(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := request(now)
	old, grant, err := transitionSnapshot(nil, now, airlock.Command{Kind: airlock.Submit, Request: r})
	if err != nil {
		t.Fatal(err)
	}

	updated, _, err := transitionSnapshot(old, now.Add(r.Config.LeaseTTL/2), airlock.Command{Kind: airlock.Renew, Request: r, LeaseID: grant.LeaseID})
	if err != nil {
		t.Fatal(err)
	}

	at := now.Add(r.Config.LeaseTTL)
	m.ExpectQuery("SELECT r.config").WillReturnRows(observationRows(old, r, at))
	m.ExpectBegin()
	m.ExpectQuery("SELECT resource").WillReturnRows(sqlmock.NewRows([]string{"resource"}).AddRow(r.Resource))
	m.ExpectQuery("SELECT r.config").WillReturnRows(observationRows(updated, r, at))
	m.ExpectRollback()
	got, err := d.Observe(context.Background(), r)
	if err != nil || got.Status != airlock.Granted || got.Revision != grant.Revision+1 {
		t.Fatalf("%+v %v", got, err)
	}
}

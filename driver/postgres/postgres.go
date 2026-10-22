// Package postgres persists Airlock's durable admission protocol in PostgreSQL.
// Callers own the database pool. New performs no I/O; EnsureSchema is explicit.
// Commands lock one permanent resource row and evaluate backend time after the
// lock, then persist the shared state machine's snapshot in the same transaction.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tr1v3r/airlock"
)

// Driver is a concurrency-safe PostgreSQL adapter. Construct it with New; its
// zero value is invalid. It never closes the injected pool or starts workers.
type Driver struct {
	db      *sql.DB
	prefix  string
	table   string
	timeout time.Duration
}

// Option configures a driver before use.
type Option func(*Driver) error

// WithTable selects table or schema.table. Unqualified tables use public.
// Components must be ASCII SQL identifiers of at most 63 bytes; quoting makes
// them case-sensitive. Provision the schema separately.
func WithTable(name string) Option {
	return func(d *Driver) error {
		parts := strings.Split(name, ".")
		if len(parts) == 1 {
			parts = append([]string{"public"}, parts...)
		}
		if len(parts) != 2 {
			return errors.New("airlock/postgres: invalid table name")
		}
		for i, part := range parts {
			if len(part) == 0 || len(part) > 63 {
				return errors.New("airlock/postgres: invalid table name")
			}
			for j, c := range part {
				valid := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (j > 0 && c >= '0' && c <= '9')
				if !valid {
					return errors.New("airlock/postgres: invalid table name")
				}
			}
			parts[i] = `"` + part + `"`
		}
		d.table = strings.Join(parts, ".")
		return nil
	}
}

// WithOperationTimeout bounds each database operation, including lock waits and
// schema creation. The default is five seconds. A caller deadline may be earlier.
// The SQL driver must support context cancellation and network timeouts.
func WithOperationTimeout(timeout time.Duration) Option {
	return func(d *Driver) error {
		if timeout <= 0 {
			return errors.New("airlock/postgres: timeout must be positive")
		}
		d.timeout = timeout
		return nil
	}
}

// New constructs a driver without connecting or running DDL. The default table
// is public.airlock_resources. Prefix is an independent namespace column, so
// prefix/resource boundaries cannot collide. Prefix must be valid UTF-8 without
// NUL and at most 256 bytes. All clients sharing a resource must share its config.
func New(db *sql.DB, prefix string, options ...Option) (*Driver, error) {
	if db == nil {
		return nil, errors.New("airlock/postgres: nil database")
	}
	if len(prefix) > 256 || !utf8.ValidString(prefix) || strings.ContainsRune(prefix, 0) {
		return nil, errors.New("airlock/postgres: invalid prefix")
	}
	d := &Driver{db: db, prefix: prefix, table: `"public"."airlock_resources"`, timeout: 5 * time.Second}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("airlock/postgres: nil option")
		}
		if err := option(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// EnsureSchema explicitly creates a logged table if absent; it does not migrate
// or validate an existing table. Prefer serialized production migrations, since
// concurrent DDL can race in PostgreSQL catalogs. Do not add triggers or external
// writers. Resource rows preserve config and scheduling history and are never
// deleted automatically. Administrative deletion requires quiescing all clients.
func (d *Driver) EnsureSchema(ctx context.Context) error {
	if err := d.validate(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	_, err := d.db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 namespace text COLLATE pg_catalog."C" NOT NULL,
 resource text COLLATE pg_catalog."C" NOT NULL,
 snapshot bytea NOT NULL,
 PRIMARY KEY (namespace, resource)
)`, d.table))
	if err != nil {
		return fmt.Errorf("airlock/postgres: schema: %w", err)
	}
	return nil
}

// Execute atomically evaluates a command. Business rejections still commit any
// expiry/scheduling changes. Transport/commit errors return a zero Result: the
// transaction may have committed, so recover with the same request identity.
// No connection is held while the admitted business task executes.
func (d *Driver) Execute(ctx context.Context, cmd airlock.Command) (airlock.Result, error) {
	if err := d.validate(ctx); err != nil {
		return airlock.Result{}, err
	}
	if err := airlock.ValidateCommand(cmd); err != nil {
		return airlock.Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return airlock.Result{}, fmt.Errorf("airlock/postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	key := cmd.Request.Resource
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (namespace, resource, snapshot) VALUES ($1, $2, $3) ON CONFLICT (namespace, resource) DO NOTHING`, d.table), d.prefix, key, []byte{}); err != nil {
		return airlock.Result{}, fmt.Errorf("airlock/postgres: initialize: %w", err)
	}
	var snapshot []byte
	if err = tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT snapshot FROM %s WHERE namespace = $1 AND resource = $2 FOR UPDATE`, d.table), d.prefix, key).Scan(&snapshot); err != nil {
		return airlock.Result{}, fmt.Errorf("airlock/postgres: lock: %w", err)
	}
	// A separate statement is necessary: expressions in the locking SELECT may
	// execute before waiting for its lock. Transaction timestamps are also stale.
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT pg_catalog.clock_timestamp()`).Scan(&now); err != nil {
		return airlock.Result{}, fmt.Errorf("airlock/postgres: clock: %w", err)
	}
	next, result, transitionErr := airlock.Transition(snapshot, now, cmd)
	if next == nil {
		return result, transitionErr
	}
	changed, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET snapshot = $3 WHERE namespace = $1 AND resource = $2`, d.table), d.prefix, key, next)
	if err != nil {
		return airlock.Result{}, fmt.Errorf("airlock/postgres: persist: %w", err)
	}
	count, err := changed.RowsAffected()
	if err != nil {
		return airlock.Result{}, fmt.Errorf("airlock/postgres: persist count: %w", err)
	}
	if count != 1 {
		return airlock.Result{}, errors.New("airlock/postgres: resource row disappeared")
	}
	if err = tx.Commit(); err != nil {
		return airlock.Result{}, fmt.Errorf("airlock/postgres: commit: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return airlock.Result{}, err
	}
	return result, transitionErr
}

func (d *Driver) validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d == nil || d.db == nil {
		return errors.New("airlock/postgres: uninitialized driver")
	}
	return nil
}

var _ airlock.Driver = (*Driver)(nil)

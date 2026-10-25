// Package postgres persists Airlock's durable admission protocol in PostgreSQL.
// Callers own the database pool. New performs no I/O; EnsureSchema is explicit.
// Commands lock one permanent resource row and evaluate backend time after the
// lock, then persist resource metadata and changed request rows in the same transaction.
// Methods require non-nil contexts and follow airlock.Driver recovery rules.
// An error never authorizes work; a commit response can be lost after persistence.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tr1v3r/airlock/protocol"
)

// Driver is a concurrency-safe PostgreSQL adapter. Construct it with New; its
// zero value is invalid. It never closes the injected pool or starts workers.
type Driver struct {
	// db is the caller-owned pool; operations use short resource-row transactions.
	db *sql.DB
	// prefix is stored in a separate namespace column, preserving resource boundaries.
	prefix string
	// table is the validated and quoted schema-qualified resource table.
	table string
	// requests is the validated and quoted request table.
	requests string
	// timeout caps database calls and transaction lock waits.
	timeout time.Duration
}

// Option configures a driver before use.
type Option func(*Driver) error

// WithTable selects table or schema.table. Unqualified tables use public.
// The request table is named <table>_requests (default airlock_resources_requests).
// Schema identifiers allow 63 bytes; table identifiers allow 54 for the suffix.
// Components must be ASCII SQL identifiers; quoting makes
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
			if len(part) == 0 || len(part) > 63 || (i == 1 && len(part) > 54) {
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
		d.requests = parts[0] + "." + strings.TrimSuffix(parts[1], `"`) + `_requests"`
		return nil
	}
}

// WithOperationTimeout bounds each database operation, including lock waits and
// schema creation. The default is five seconds. A caller deadline may be earlier.
// The timeout must be positive. The SQL driver must support context cancellation
// and network timeouts.
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
	d := &Driver{db: db, prefix: prefix, table: `"public"."airlock_resources"`, requests: `"public"."airlock_resources_requests"`, timeout: 5 * time.Second}
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

// execute atomically evaluates a command. Business rejections still commit any
// expiry/scheduling changes. Transport/commit errors return a zero Result: the
// transaction may have committed, so recover with the same request identity.
// No connection is held while the admitted business task executes.
func (d *Driver) execute(ctx context.Context, cmd protocol.Command) (protocol.Result, error) {
	if err := d.validate(ctx); err != nil {
		return protocol.Result{}, err
	}
	if err := protocol.ValidateCommand(cmd); err != nil {
		return protocol.Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return protocol.Result{}, fmt.Errorf("airlock/postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	state, before, err := d.load(ctx, tx, cmd.Request)
	if err != nil {
		return protocol.Result{}, err
	}
	// Evaluate time after the resource lock AND reading its request rows. The
	// transaction start time must not revive a lease after a long lock wait.
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT pg_catalog.clock_timestamp()`).Scan(&now); err != nil {
		return protocol.Result{}, fmt.Errorf("airlock/postgres: clock: %w", err)
	}
	next, result, _, transitionErr := protocol.Transition(state, now, cmd)
	if next == nil {
		return result, transitionErr
	}
	if err = d.persist(ctx, tx, next, before); err != nil {
		return protocol.Result{}, err
	}
	if err = tx.Commit(); err != nil {
		return protocol.Result{}, fmt.Errorf("airlock/postgres: commit: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return protocol.Result{}, err
	}
	return result, transitionErr
}

// validate checks caller cancellation and driver initialization before database I/O.
func (d *Driver) validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d == nil || d.db == nil {
		return errors.New("airlock/postgres: uninitialized driver")
	}
	return nil
}

// Verify the PostgreSQL backend implements the public Driver contract.
var _ protocol.Driver = (*Driver)(nil)

// Submit atomically submits the whole request without refreshing an existing lease.
func (d *Driver) Submit(ctx context.Context, request protocol.Request) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Submit, Request: request})
}

// Observe returns the request outcome after advancing expiry and scheduling.
func (d *Driver) Observe(ctx context.Context, request protocol.Request) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Observe, Request: request})
}

// Cancel cancels the request, including a grant or a cancellation before submission.
func (d *Driver) Cancel(ctx context.Context, request protocol.Request) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Cancel, Request: request})
}

// Renew extends a live lease matching the original request and lease ID.
func (d *Driver) Renew(ctx context.Context, lease protocol.LeaseRef) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Renew, Request: lease.Request, LeaseID: lease.ID})
}

// Release ends a matching lease and returns its whole weight at most once.
func (d *Driver) Release(ctx context.Context, lease protocol.LeaseRef) (protocol.Result, error) {
	return d.execute(ctx, protocol.Command{Kind: protocol.Release, Request: lease.Request, LeaseID: lease.ID})
}

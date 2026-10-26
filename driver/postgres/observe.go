package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/tr1v3r/airlock"
)

type observerQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// observe reads one resource header and one request in a single MVCC statement.
// It takes no row lock and never loads rotations or other request records.
func (d *Driver) observe(ctx context.Context, q observerQuery, request airlock.Request) (airlock.Result, bool, error) {
	query := fmt.Sprintf(`
		SELECT
			r.config, r.observed_ms, r.revision, r.next_maintenance_ms,
			q.id, q.owner, q.tenant, q.weight, q.submit_until_ms, q.wait_until_ms,
			q.status, q.lease_id, q.until_ms, q.finished_ms, q.failure,
			pg_catalog.clock_timestamp()
		FROM %s r
		LEFT JOIN %s q
			ON q.namespace = r.namespace
			AND q.resource = r.resource
			AND q.id = $3
		WHERE r.namespace = $1
			AND r.resource = $2
	`, d.resourceTable, d.requestTable)

	var config []byte
	var observed, next sql.NullInt64
	var revision string
	var now time.Time

	var id, owner, tenant, status, lease, failure sql.NullString
	var weight, submit, wait, until, finished sql.NullInt64

	err := q.QueryRowContext(ctx, query, d.namespace, request.Resource, request.ID).Scan(
		&config, &observed, &revision, &next,
		&id, &owner, &tenant, &weight, &submit, &wait,
		&status, &lease, &until, &finished, &failure,
		&now,
	)
	if err == sql.ErrNoRows {
		return airlock.Result{}, false, nil
	}
	if err != nil {
		return airlock.Result{}, true, fmt.Errorf("airlock/postgres: observe: %w", err)
	}

	o := airlock.Observation{
		Resource:        request.Resource,
		Time:            instant(observed),
		NextMaintenance: instant(next),
	}
	o.Revision, err = strconv.ParseUint(revision, 10, 64)
	if err != nil || json.Unmarshal(config, &o.Config) != nil {
		return airlock.Result{}, true, airlock.ErrCorruptState
	}

	// An uninitialized resource is completed by the ordinary transition path.
	if o.Revision == 0 {
		return airlock.Result{}, false, nil
	}

	if id.Valid {
		if !owner.Valid || !tenant.Valid || !weight.Valid ||
			!submit.Valid || !wait.Valid || !status.Valid ||
			!lease.Valid || !failure.Valid {
			return airlock.Result{}, true, airlock.ErrCorruptState
		}

		r := request
		r.ID, r.Owner, r.Tenant, r.Weight = id.String, owner.String, tenant.String, int(weight.Int64)
		r.SubmitUntil = time.UnixMilli(submit.Int64).UTC()
		r.WaitUntil = time.UnixMilli(wait.Int64).UTC()

		o.Record = &airlock.DriverRecord{
			Request:  r,
			Status:   airlock.Status(status.String),
			LeaseID:  lease.String,
			Until:    instant(until),
			Finished: instant(finished),
			Failure:  failure.String,
		}
	}

	result, ready, err := o.Observe(now, request)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return airlock.Result{}, true, ctxErr
	}
	return result, ready, err
}

// observeLocked rechecks maintenance after joining the resource's serialization
// boundary. A new SQL statement after lock acquisition avoids mixing an updated
// header with request rows from a snapshot taken before a lock wait.
func (d *Driver) observeLocked(ctx context.Context, tx *sql.Tx, request airlock.Request) (airlock.Result, bool, error) {
	query := fmt.Sprintf(`
		SELECT resource
		FROM %s
		WHERE namespace = $1
			AND resource = $2
		FOR UPDATE
	`, d.resourceTable)

	var resource string
	err := tx.QueryRowContext(ctx, query, d.namespace, request.Resource).Scan(&resource)
	if err == sql.ErrNoRows {
		return airlock.Result{}, false, nil
	}
	if err != nil {
		return airlock.Result{}, true, err
	}

	return d.observe(ctx, tx, request)
}

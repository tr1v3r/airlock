package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/tr1v3r/airlock"
)

// Resize uses the same resource row lock and transaction as business commands.
func (d *Driver) Resize(ctx context.Context, request airlock.ResizeRequest) (airlock.ResourceStatus, error) {
	r, err := d.execute(ctx, airlock.Command{Kind: airlock.Resize, Resize: request})
	if err != nil {
		return airlock.ResourceStatus{}, err
	}
	return r.ResourceStatus, nil
}

// InspectResource reads header and charged counters in one MVCC statement. It
// neither locks the resource for update nor reclaims expired records.
func (d *Driver) InspectResource(ctx context.Context, resource string) (airlock.ResourceStatus, error) {
	if err := d.validate(ctx); err != nil {
		return airlock.ResourceStatus{}, err
	}
	if err := airlock.ValidateResource(resource); err != nil {
		return airlock.ResourceStatus{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	query := fmt.Sprintf(`SELECT r.capacity, r.capacity_version, r.revision, r.observed_ms,
		COALESCE((SELECT SUM(q.weight) FROM %s q WHERE q.namespace=r.namespace AND q.resource=r.resource AND q.status='granted'),0),
		(SELECT COUNT(*) FROM %s q WHERE q.namespace=r.namespace AND q.resource=r.resource AND q.status='queued'),
		pg_catalog.clock_timestamp()
		FROM %s r WHERE r.namespace=$1 AND r.resource=$2`, d.requestTable, d.requestTable, d.resourceTable)
	o := airlock.Observation{Resource: resource}
	var version, revision string
	var observed sql.NullInt64
	var now time.Time
	err := d.db.QueryRowContext(ctx, query, d.namespace, resource).Scan(&o.Capacity, &version, &revision, &observed, &o.Used, &o.Queued, &now)
	if err == sql.ErrNoRows {
		return airlock.ResourceStatus{}, airlock.ErrResourceNotFound
	}
	if err != nil {
		return airlock.ResourceStatus{}, fmt.Errorf("airlock/postgres: inspect: %w", err)
	}
	o.CapacityVersion, err = strconv.ParseUint(version, 10, 64)
	if err != nil {
		return airlock.ResourceStatus{}, airlock.ErrCorruptState
	}
	o.Revision, err = strconv.ParseUint(revision, 10, 64)
	if err != nil {
		return airlock.ResourceStatus{}, airlock.ErrCorruptState
	}
	if o.Revision == 0 {
		return airlock.ResourceStatus{}, airlock.ErrResourceNotFound
	}
	o.Time = instant(observed)
	if err := ctx.Err(); err != nil {
		return airlock.ResourceStatus{}, err
	}
	return o.Inspect(now, resource)
}

var _ airlock.ResourceManager = (*Driver)(nil)

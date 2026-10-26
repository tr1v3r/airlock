package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/tr1v3r/airlock"
)

// row is a value copy so Transition cannot mutate the delta baseline.
type row struct {
	record   airlock.DriverRecord
	position sql.NullInt64
}

func millis(t time.Time) sql.NullInt64 {
	return sql.NullInt64{Int64: t.UnixMilli(), Valid: !t.IsZero()}
}
func instant(n sql.NullInt64) time.Time {
	if !n.Valid {
		return time.Time{}
	}
	return time.UnixMilli(n.Int64).UTC()
}

const resourceColumns = `state_version, config, rotations, protected, cursor, revision, observed_ms`
const requestColumns = `id, owner, tenant, weight, submit_until_ms, wait_until_ms, status, lease_id, until_ms, finished_ms, failure, queue_position`

func (d *Driver) load(ctx context.Context, tx *sql.Tx, request airlock.Request) (*airlock.SharedState, map[string]row, error) {
	config, err := json.Marshal(request.Config)
	if err != nil {
		return nil, nil, err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (namespace, resource, config) VALUES ($1,$2,$3) ON CONFLICT (namespace, resource) DO NOTHING`, d.table), d.namespace, request.Resource, string(config))
	if err != nil {
		return nil, nil, fmt.Errorf("airlock/postgres: initialize: %w", err)
	}
	s := &airlock.SharedState{Resource: request.Resource, Records: make(map[string]*airlock.DriverRecord), Queues: make(map[string][]string)}
	var rotations []byte
	var revision string
	var observed sql.NullInt64
	err = tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE namespace=$1 AND resource=$2 FOR UPDATE`, resourceColumns, d.table), d.namespace, request.Resource).Scan(&s.Version, &config, &rotations, &s.Protected, &s.Cursor, &revision, &observed)
	if err != nil {
		return nil, nil, fmt.Errorf("airlock/postgres: lock: %w", err)
	}
	s.Revision, err = strconv.ParseUint(revision, 10, 64)
	if err != nil || json.Unmarshal(config, &s.Config) != nil || json.Unmarshal(rotations, &s.Rotations) != nil {
		return nil, nil, airlock.ErrCorruptState
	}
	s.Time = instant(observed)
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE namespace=$1 AND resource=$2 ORDER BY tenant, queue_position NULLS LAST, id LIMIT 10001`, requestColumns, d.requests), d.namespace, request.Resource)
	if err != nil {
		return nil, nil, fmt.Errorf("airlock/postgres: read requests: %w", err)
	}
	defer func() { _ = rows.Close() }()
	before := make(map[string]row)
	for rows.Next() {
		var rec airlock.DriverRecord
		var submit, wait int64
		var until, finished, position sql.NullInt64
		rec.Request.Resource = request.Resource
		err = rows.Scan(&rec.Request.ID, &rec.Request.Owner, &rec.Request.Tenant, &rec.Request.Weight, &submit, &wait, &rec.Status, &rec.LeaseID, &until, &finished, &rec.Failure, &position)
		if err != nil {
			return nil, nil, fmt.Errorf("airlock/postgres: decode request: %w", err)
		}
		rec.Request.SubmitUntil = time.UnixMilli(submit).UTC()
		rec.Request.WaitUntil = time.UnixMilli(wait).UTC()
		rec.Until, rec.Finished = instant(until), instant(finished)
		if (rec.Status == airlock.Queued) != position.Valid {
			return nil, nil, airlock.ErrCorruptState
		}
		if position.Valid {
			queue := s.Queues[rec.Request.Tenant]
			if position.Int64 < 0 || (len(queue) > 0 && position.Int64 <= before[queue[len(queue)-1]].position.Int64) {
				return nil, nil, airlock.ErrCorruptState
			}
			s.Queues[rec.Request.Tenant] = append(queue, rec.Request.ID)
		}
		before[rec.Request.ID] = row{record: rec, position: position}
		s.Records[rec.Request.ID] = &rec
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("airlock/postgres: read requests: %w", err)
	}
	if s.Revision == 0 {
		if len(before) != 0 || observed.Valid || s.Version != 1 || s.Protected != "" || s.Cursor != 0 || len(s.Rotations) != 0 {
			return nil, nil, airlock.ErrCorruptState
		}
		if !reflect.DeepEqual(s.Config, request.Config) {
			return nil, nil, airlock.ErrConfigConflict
		}
		return nil, before, nil
	}
	return s, before, nil
}

func (d *Driver) persist(ctx context.Context, tx *sql.Tx, s *airlock.SharedState, before map[string]row) error {
	rotations, err := json.Marshal(s.Rotations)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET rotations=$3, protected=$4, cursor=$5, revision=$6, observed_ms=$7, next_maintenance_ms=$8 WHERE namespace=$1 AND resource=$2`, d.table), d.namespace, s.Resource, string(rotations), s.Protected, s.Cursor, strconv.FormatUint(s.Revision, 10), millis(s.Time), millis(s.NextMaintenance()))
	if err = oneRow(result, err); err != nil {
		return fmt.Errorf("airlock/postgres: persist resource: %w", err)
	}
	positions := make(map[string]sql.NullInt64)
	for _, queue := range s.Queues {
		previous := int64(-1)
		for _, id := range queue {
			position := before[id].position
			if !position.Valid {
				if previous == math.MaxInt64 {
					return airlock.ErrCorruptState
				}
				position = sql.NullInt64{Int64: previous + 1, Valid: true}
			}
			if position.Int64 <= previous {
				return airlock.ErrCorruptState
			}
			positions[id], previous = position, position.Int64
		}
	}
	for id := range before {
		if s.Records[id] != nil {
			continue
		}
		result, err = tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE namespace=$1 AND resource=$2 AND id=$3`, d.requests), d.namespace, s.Resource, id)
		if err = oneRow(result, err); err != nil {
			return fmt.Errorf("airlock/postgres: prune request: %w", err)
		}
	}
	for id, rec := range s.Records {
		position := positions[id]
		if old, ok := before[id]; ok && old.position == position && reflect.DeepEqual(old.record, *rec) {
			continue
		}
		result, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (namespace, resource, %s) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
 ON CONFLICT (namespace,resource,id) DO UPDATE SET status=EXCLUDED.status,lease_id=EXCLUDED.lease_id,until_ms=EXCLUDED.until_ms,finished_ms=EXCLUDED.finished_ms,failure=EXCLUDED.failure,queue_position=EXCLUDED.queue_position`, d.requests, requestColumns), d.namespace, s.Resource, id, rec.Request.Owner, rec.Request.Tenant, rec.Request.Weight, rec.Request.SubmitUntil.UnixMilli(), rec.Request.WaitUntil.UnixMilli(), string(rec.Status), rec.LeaseID, millis(rec.Until), millis(rec.Finished), rec.Failure, position)
		if err = oneRow(result, err); err != nil {
			return fmt.Errorf("airlock/postgres: persist request: %w", err)
		}
	}
	return nil
}

func oneRow(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("airlock/postgres: expected one affected row, got %d", n)
	}
	return nil
}

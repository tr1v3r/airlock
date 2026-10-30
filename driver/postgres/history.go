package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/tr1v3r/airlock"
)

func terminalDeadline(rec *airlock.DriverRecord, retention time.Duration) sql.NullInt64 {
	if rec.Status == airlock.Queued || rec.Status == airlock.Granted {
		return sql.NullInt64{}
	}
	until := rec.Finished.Add(retention)
	if rec.Request.SubmitUntil.After(until) {
		until = rec.Request.SubmitUntil
	}
	return millis(until)
}

// reclaim removes indexed due history under the resource lock. The loaded target
// remains for core's conflict-before-GC check. Nil transitions roll back deletions.
// Work is O(due), not O(retained history); capped GC cannot falsely fill MaxRecords.
func (d *Driver) reclaim(ctx context.Context, tx *sql.Tx, s *airlock.SharedState, target string, now time.Time, totalRecords int) (airlock.RetainedHistory, error) {
	if s == nil || totalRecords == len(s.Records) {
		return airlock.RetainedHistory{}, nil
	}
	if s.Time.After(now) {
		now = s.Time
	}
	result, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE namespace=$1 AND resource=$2 AND status NOT IN ('queued','granted') AND gc_after_ms <= $3 AND id <> $4`, d.requestTable), d.namespace, s.Resource, now.UnixMilli(), target)
	if err != nil {
		return airlock.RetainedHistory{}, fmt.Errorf("airlock/postgres: history GC: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return airlock.RetainedHistory{}, err
	}
	if removed < 0 || removed > int64(totalRecords-len(s.Records)) {
		return airlock.RetainedHistory{}, airlock.ErrCorruptState
	}
	count := totalRecords - len(s.Records) - int(removed)
	var before sql.NullInt64
	err = tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT gc_after_ms FROM %s WHERE namespace=$1 AND resource=$2 AND gc_after_ms IS NOT NULL AND id <> $3 ORDER BY gc_after_ms,id LIMIT 1`, d.requestTable), d.namespace, s.Resource, target).Scan(&before)
	if err == sql.ErrNoRows {
		err = nil
	}
	if err != nil {
		return airlock.RetainedHistory{}, fmt.Errorf("airlock/postgres: history boundary: %w", err)
	}
	if (count == 0) != !before.Valid || (before.Valid && before.Int64 <= now.UnixMilli()) {
		return airlock.RetainedHistory{}, airlock.ErrCorruptState
	}
	return airlock.RetainedHistory{Count: count, Before: instant(before)}, nil
}

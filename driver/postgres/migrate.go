package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tr1v3r/airlock"
)

// MigrateSnapshotSchema converts the previous bytea snapshot layout in place.
// Stop ALL clients and active work first, take a backup, and run once with an
// adequate WithOperationTimeout. The exclusive table lock, DDL and all copied
// state commit together. It preserves identities, deadlines, history and order;
// it does not renew leases or reset accounting. Old binaries cannot use the new
// schema. This is an offline migration, not a rolling-upgrade protocol.
func (d *Driver) MigrateSnapshotSchema(ctx context.Context) error {
	if err := d.validate(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`LOCK TABLE %s IN ACCESS EXCLUSIVE MODE`, d.table)); err != nil {
		return err
	}
	// Fail before changing anything if this is not the legacy layout.
	check, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT snapshot FROM %s WHERE false`, d.table))
	if err != nil {
		return fmt.Errorf("airlock/postgres: migration requires the legacy snapshot schema: %w", err)
	}
	if err = check.Close(); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s
 ADD COLUMN state_version integer NOT NULL DEFAULT 1 CHECK (state_version=1),
 ADD COLUMN config jsonb,
 ADD COLUMN rotations jsonb NOT NULL DEFAULT '[]',
 ADD COLUMN protected text COLLATE pg_catalog."C" NOT NULL DEFAULT '',
 ADD COLUMN cursor integer NOT NULL DEFAULT 0 CHECK (cursor>=0),
 ADD COLUMN revision numeric(20,0) NOT NULL DEFAULT 0 CHECK (revision>=0 AND revision<=18446744073709551615),
 ADD COLUMN observed_ms bigint`, d.table))
	if err != nil {
		return err
	}
	// A preexisting request table is ambiguous: never merge it into legacy state.
	ddl := d.schema()[1]
	ddl = strings.Replace(ddl, "CREATE TABLE IF NOT EXISTS", "CREATE TABLE", 1)
	if _, err = tx.ExecContext(ctx, ddl); err != nil {
		return err
	}
	var namespace, resource string
	first := true
	for {
		rows, e := tx.QueryContext(ctx, fmt.Sprintf(`SELECT namespace,resource,snapshot FROM %s WHERE $1 OR (namespace,resource)>($2,$3) ORDER BY namespace,resource LIMIT 1`, d.table), first, namespace, resource)
		if e != nil {
			return e
		}
		if !rows.Next() {
			e = rows.Err()
			_ = rows.Close()
			if e != nil {
				return e
			}
			break
		}
		var data []byte
		e = rows.Scan(&namespace, &resource, &data)
		closeErr := rows.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
		first = false
		var s airlock.DriverState
		if json.Unmarshal(data, &s) != nil || s.Resource != resource || len(data) == 0 {
			return airlock.ErrCorruptState
		}
		if err = validateLegacy(data, s); err != nil {
			return err
		}
		config, e := json.Marshal(s.Config)
		if e != nil {
			return e
		}
		if _, err = tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET config=$3 WHERE namespace=$1 AND resource=$2`, d.table), namespace, resource, string(config)); err != nil {
			return err
		}
		scoped := *d
		scoped.prefix = namespace
		if err = scoped.persist(ctx, tx, &s, nil); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN config SET NOT NULL, DROP COLUMN snapshot`, d.table)); err != nil {
		return err
	}
	return tx.Commit()
}

func validateLegacy(data []byte, s airlock.DriverState) error {
	// Validate a separate copy through the reference decoder; discard the computed
	// transition. Migration must preserve the original time and revision exactly.
	r := airlock.NewRequest(s.Time.Add(s.Config.SubmitWindow))
	r.Resource, r.Config, r.Weight, r.WaitUntil = s.Resource, s.Config, 1, s.Time.Add(s.Config.MaxWait)
	for _, record := range s.Records {
		if record != nil {
			r = record.Request
			r.Config = s.Config
			break
		}
	}
	next, _, err := airlock.Transition(data, s.Time, airlock.Command{Kind: airlock.Observe, Request: r})
	if next == nil {
		return fmt.Errorf("airlock/postgres: invalid legacy state: %w", err)
	}
	return nil
}

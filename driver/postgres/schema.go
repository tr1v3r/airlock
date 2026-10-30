package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
)

// EnsureSchema explicitly creates the two relational tables in one transaction.
// It checks required columns and migrates the legacy capacity > 0 constraint to
// capacity >= 0, allowing persisted pauses. It does not create a schema.
// Serialize deployment DDL; upgrade all clients before using Resize(0).
func (d *Driver) EnsureSchema(ctx context.Context) error {
	if err := d.validate(ctx); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("airlock/postgres: schema begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, ddl := range d.schema() {
		if _, err = tx.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("airlock/postgres: schema: %w", err)
		}
	}

	checks := []string{
		fmt.Sprintf(`SELECT %s, next_maintenance_ms FROM %s WHERE false`, resourceColumns, d.resourceTable),
		fmt.Sprintf(`SELECT %s FROM %s WHERE false`, requestColumns, d.requestTable),
	}

	for _, query := range checks {
		rows, e := tx.QueryContext(ctx, query)
		if e != nil {
			return fmt.Errorf("airlock/postgres: incompatible schema: %w", e)
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *Driver) schema() []string {
	return []string{
		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			namespace              text COLLATE pg_catalog."C" NOT NULL,
			resource               text COLLATE pg_catalog."C" NOT NULL,
			state_version          integer NOT NULL DEFAULT 1 CHECK (state_version=1),
			config                 jsonb NOT NULL,
			capacity               bigint NOT NULL CHECK (capacity>=0),
			capacity_version       numeric(20,0) NOT NULL DEFAULT 1 CHECK (capacity_version>=1 AND capacity_version<=18446744073709551615),
			rotations              jsonb NOT NULL DEFAULT '[]',
			protected              text COLLATE pg_catalog."C" NOT NULL DEFAULT '',
			cursor                 integer NOT NULL DEFAULT 0 CHECK (cursor>=0),
			revision               numeric(20,0) NOT NULL DEFAULT 0 CHECK (revision>=0 AND revision<=18446744073709551615),
			observed_ms            bigint,
			next_maintenance_ms    bigint,
            total_records          bigint NOT NULL DEFAULT 0 CHECK (total_records >= 0 AND total_records <= 1000000),

			PRIMARY KEY (namespace, resource)
		)
		`, d.resourceTable),

		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			namespace             text COLLATE pg_catalog."C" NOT NULL,
			resource              text COLLATE pg_catalog."C" NOT NULL,
			id                    text COLLATE pg_catalog."C" NOT NULL,
			owner                 text NOT NULL,
			tenant                text COLLATE pg_catalog."C" NOT NULL,
			weight                bigint NOT NULL CHECK (weight>0),
			submit_until_ms       bigint NOT NULL,
			wait_until_ms         bigint NOT NULL,
			status                text NOT NULL CHECK (status IN ('queued','granted','released','expired','cancelled','rejected')),
			lease_id              text NOT NULL,
			until_ms              bigint,
			finished_ms           bigint,
			failure               text NOT NULL,
			queue_position        bigint CHECK (queue_position>=0),
            gc_after_ms           bigint,

			PRIMARY KEY (namespace, resource, id),
			FOREIGN KEY (namespace, resource) REFERENCES %s (namespace, resource),
			UNIQUE (namespace, resource, tenant, queue_position) DEFERRABLE INITIALLY DEFERRED,
			CHECK ((status='queued')=(queue_position IS NOT NULL)),
            CHECK ((status IN ('queued','granted'))=(gc_after_ms IS NULL))
		)
		`, d.requestTable, d.resourceTable),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (namespace,resource,tenant,queue_position,id) WHERE status IN ('queued','granted')`, d.historyIndex("active"), d.requestTable),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (namespace,resource,gc_after_ms,id) WHERE gc_after_ms IS NOT NULL`, d.historyIndex("terminal"), d.requestTable),

		// Match only the legacy capacity check, not unrelated operator constraints.
		// Reuse its actual name (PostgreSQL may truncate generated identifiers).
		fmt.Sprintf(`DO $airlock$
		DECLARE legacy record;
		BEGIN
			FOR legacy IN SELECT conname FROM pg_catalog.pg_constraint
			WHERE conrelid = '%s'::regclass AND contype = 'c'
			AND regexp_replace(pg_catalog.pg_get_expr(conbin, conrelid), '[[:space:]()]', '', 'g')
			    IN ('capacity>0', 'capacity>0::bigint')
			LOOP
				EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %%I', legacy.conname);
				EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %%I CHECK (capacity>=0)', legacy.conname);
			END LOOP;
		END $airlock$`, d.resourceTable, d.resourceTable, d.resourceTable),
	}
}

// Hash qualified table identity so PostgreSQL name truncation cannot alias indexes.
func (d *Driver) historyIndex(kind string) string {
	return fmt.Sprintf("airlock_%s_%x", kind, sha256.Sum256([]byte(d.requestTable)))[:55]
}

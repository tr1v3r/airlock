package postgres

import (
	"context"
	"fmt"
)

// EnsureSchema explicitly creates the two logged tables in one transaction. It
// rejects the old snapshot layout; see README for an offline migration. It does
// not change an existing layout or create a schema. Serialize deployment DDL.
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
	for _, query := range []string{fmt.Sprintf(`SELECT %s FROM %s WHERE false`, resourceColumns, d.table), fmt.Sprintf(`SELECT %s FROM %s WHERE false`, requestColumns, d.requests)} {
		rows, e := tx.QueryContext(ctx, query)
		if e != nil {
			return fmt.Errorf("airlock/postgres: incompatible schema (snapshot tables require offline migration; see README): %w", e)
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *Driver) schema() []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 namespace text COLLATE pg_catalog."C" NOT NULL,
 resource text COLLATE pg_catalog."C" NOT NULL,
 state_version integer NOT NULL DEFAULT 1 CHECK (state_version=1),
 config jsonb NOT NULL,
 rotations jsonb NOT NULL DEFAULT '[]',
 protected text COLLATE pg_catalog."C" NOT NULL DEFAULT '',
 cursor integer NOT NULL DEFAULT 0 CHECK (cursor>=0),
 revision numeric(20,0) NOT NULL DEFAULT 0 CHECK (revision>=0 AND revision<=18446744073709551615),
 observed_ms bigint,
 PRIMARY KEY (namespace,resource)
)`, d.table),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 namespace text COLLATE pg_catalog."C" NOT NULL,
 resource text COLLATE pg_catalog."C" NOT NULL,
 id text COLLATE pg_catalog."C" NOT NULL,
 owner text NOT NULL,
 tenant text COLLATE pg_catalog."C" NOT NULL,
 weight integer NOT NULL CHECK (weight>0),
 submit_until_ms bigint NOT NULL,
 wait_until_ms bigint NOT NULL,
 status text NOT NULL CHECK (status IN ('queued','granted','released','expired','cancelled','rejected')),
 lease_id text NOT NULL,
 until_ms bigint,
 finished_ms bigint,
 failure text NOT NULL,
 queue_position bigint CHECK (queue_position>=0),
 PRIMARY KEY (namespace,resource,id),
 FOREIGN KEY (namespace,resource) REFERENCES %s(namespace,resource),
 UNIQUE (namespace,resource,tenant,queue_position) DEFERRABLE INITIALLY DEFERRED,
 CHECK ((status='queued')=(queue_position IS NOT NULL))
)`, d.requests, d.table),
	}
}

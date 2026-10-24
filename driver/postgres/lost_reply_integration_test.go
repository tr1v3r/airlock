package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/internal/driverconformance"
)

// The database really commits; only its driver-level acknowledgement is dropped.
// This is a deterministic reply-loss boundary, not a TCP partition simulation.
type replyLossConnector struct {
	driver.Connector
	armed atomic.Bool
}

func (c *replyLossConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &replyLossConn{Conn: conn, armed: &c.armed}, nil
}

type replyLossConn struct {
	driver.Conn
	armed *atomic.Bool
}

func (c *replyLossConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return &replyLossTx{Tx: tx, armed: c.armed}, nil
}
func (c *replyLossConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *replyLossConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type replyLossTx struct {
	driver.Tx
	armed *atomic.Bool
}

func (tx *replyLossTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.armed.Swap(false) {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func TestIntegrationCommittedResponseLoss(t *testing.T) {
	normalDB := integrationDB(t)
	base, err := stdlib.GetDefaultDriver().(driver.DriverContext).OpenConnector(os.Getenv("AIRLOCK_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	connector := &replyLossConnector{Connector: base}
	faultyDB := sql.OpenDB(connector)
	t.Cleanup(func() { _ = faultyDB.Close() })
	prefix := uniquePrefix()
	faulty := integrationDriver(t, faultyDB, prefix)
	observer := integrationDriver(t, normalDB, prefix)
	r := request(time.Now())
	r.Config.LeaseTTL = 5 * time.Second
	r.Config.Admission.Capacity = 2
	r.Weight = 2
	unknown := func(kind airlock.Operation, lease string) {
		t.Helper()
		connector.armed.Store(true)
		result, e := driverconformance.Invoke(context.Background(), faulty, airlock.Command{Kind: kind, Request: r, LeaseID: lease})
		if !errors.Is(e, io.ErrUnexpectedEOF) || result != (airlock.Result{}) {
			t.Fatalf("unknown %s exposed result: %+v %v", kind, result, e)
		}
	}
	unknown(airlock.Submit, "")
	grant := executeStatus(t, observer, airlock.Observe, r, "", airlock.Granted)
	retry := executeStatus(t, observer, airlock.Submit, r, "", airlock.Granted)
	if retry.LeaseID != grant.LeaseID || retry.Remaining > grant.Remaining {
		t.Fatal("lost Submit response caused a second grant/extended TTL")
	}
	other := r
	identity := airlock.NewRequest(r.SubmitUntil)
	other.ID, other.Owner = identity.ID, identity.Owner
	other.Weight = 1
	executeStatus(t, observer, airlock.Submit, other, "", airlock.Queued)
	unknown(airlock.Renew, grant.LeaseID)
	renewed := executeStatus(t, observer, airlock.Observe, r, "", airlock.Granted)
	if renewed.LeaseID != grant.LeaseID {
		t.Fatal("renew changed identity")
	}
	unknown(airlock.Release, grant.LeaseID)
	executeStatus(t, observer, airlock.Release, r, grant.LeaseID, airlock.Released)
	next := executeStatus(t, observer, airlock.Observe, other, "", airlock.Granted)
	executeStatus(t, observer, airlock.Release, other, next.LeaseID, airlock.Released)
}

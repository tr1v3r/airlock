package postgres_test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/driver/postgres"
)

func ExampleNew() {
	// The application selects its SQL driver and owns the connection pool.
	db, err := sql.Open("pgx", os.Getenv("AIRLOCK_POSTGRES_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	driver, err := postgres.New(db, "my-service", postgres.WithOperationTimeout(2*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	// Run schema migrations explicitly during deployment, before serving traffic.
	if err := driver.EnsureSchema(context.Background()); err != nil {
		log.Fatal(err)
	}
	client, err := airlock.NewDistributed(driver, airlock.DistributedOptions{
		Resource:         "workers",
		Config:           airlock.ResourceConfig{Admission: airlock.Config{Capacity: 4, MaxQueued: 8, MaxQueuedPerTenant: 8}, LeaseTTL: time.Second, MaxWait: time.Second, SubmitWindow: time.Second, Retention: time.Minute, MaxRecords: 100},
		OperationTimeout: time.Second, PollInterval: 20 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	lease, err := client.AcquireNFor(context.Background(), "online", 2)
	if err != nil {
		log.Fatal(err)
	}
	// Do bounded work here; stop when lease.Done closes, or renew explicitly.
	// Release only after work has actually stopped. The driver runs no workers.
	if err := lease.Release(context.Background()); err != nil {
		log.Fatal(err)
	}
}

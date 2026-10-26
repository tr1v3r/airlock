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
	"github.com/tr1v3r/airlock/protocol"
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
	// Create the schema explicitly during deployment, before serving traffic.
	if err := driver.EnsureSchema(context.Background()); err != nil {
		log.Fatal(err)
	}
	client, err := airlock.NewShared(driver, airlock.SharedOptions{
		Resource: "workers",
		Config: protocol.ResourceConfig{
			Admission: protocol.Config{Capacity: 4, MaxQueued: 8, MaxQueuedPerTenant: 8},
		},
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
	cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lease.Release(cleanup); err != nil {
		log.Fatal(err)
	}
}

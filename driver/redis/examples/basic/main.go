// Run against an isolated Redis instance with AIRLOCK_REDIS_ADDR=127.0.0.1:6379.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/airlock"
	redisdriver "github.com/tr1v3r/airlock/driver/redis"
)

// main runs the Redis example only when an isolated backend address is supplied.
func main() {
	address := os.Getenv("AIRLOCK_REDIS_ADDR")
	if address == "" {
		fmt.Println("set AIRLOCK_REDIS_ADDR to run the Redis example")
		return
	}
	if err := run(address); err != nil {
		log.Fatal(err)
	}
}

// run acquires weighted shared capacity and releases after the sample work has stopped.
func run(address string) error {
	client := goredis.NewClient(&goredis.Options{Addr: address, ContextTimeoutEnabled: true})
	defer func() { _ = client.Close() }()
	driver, err := redisdriver.New(client, "example")
	if err != nil {
		return err
	}

	limiter, err := airlock.NewShared(driver, airlock.SharedOptions{
		Resource: "jobs",
		Config: airlock.ResourceConfig{
			Admission: airlock.Config{Capacity: 8, MaxQueued: 32, MaxQueuedPerTenant: 8},
		},
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease, err := limiter.AcquireNFor(ctx, "tenant-a", 2)
	if err != nil {
		return err
	}

	// Real work must stop when Done closes or Valid becomes false. Acquisition
	// context cancellation does not stop work or release this delivered lease.
	var workErr error

	select {
	case <-lease.Done():
		workErr = airlock.ErrLeaseLost
	default:
		fmt.Printf("admitted %d tokens\n", lease.Weight())
	}

	// Work has stopped before releasing. Use a fresh context for cleanup.
	cleanup, cancelCleanup := context.WithTimeout(context.Background(), time.Second)
	defer cancelCleanup()
	return errors.Join(workErr, lease.Release(cleanup))
}

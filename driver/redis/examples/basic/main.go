// Run against an isolated Redis instance with AIRLOCK_REDIS_ADDR=127.0.0.1:6379.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/tr1v3r/airlock"
	redisdriver "github.com/tr1v3r/airlock/driver/redis"
)

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
func run(address string) error {
	client := goredis.NewClient(&goredis.Options{Addr: address, ContextTimeoutEnabled: true})
	defer func() { _ = client.Close() }()
	driver, err := redisdriver.New(client, "example")
	if err != nil {
		return err
	}
	limiter, err := airlock.NewDistributed(driver, airlock.DistributedOptions{
		Resource: "jobs", OperationTimeout: time.Second, PollInterval: 20 * time.Millisecond,
		Config: airlock.ResourceConfig{Admission: airlock.Config{Capacity: 8, MaxQueued: 32, MaxQueuedPerTenant: 8}, LeaseTTL: 30 * time.Second, MaxWait: 10 * time.Second, SubmitWindow: 5 * time.Second, Retention: time.Minute, MaxRecords: 256},
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
	select {
	case <-lease.Done():
		return airlock.ErrLeaseLost
	default:
		fmt.Printf("admitted %d tokens\n", lease.Weight())
	}
	// Work has stopped before releasing. Use a fresh context for cleanup.
	cleanup, cancelCleanup := context.WithTimeout(context.Background(), time.Second)
	defer cancelCleanup()
	return lease.Release(cleanup)
}

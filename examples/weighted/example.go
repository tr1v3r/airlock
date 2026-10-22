// Run with: go run ./examples/weighted
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/tr1v3r/airlock"
)

func run(ctx context.Context) error {
	lock, err := airlock.New(airlock.Config{
		Capacity: 5, MaxQueued: 8, MaxQueuedPerTenant: 4,
	})
	if err != nil {
		return err
	}
	// Omit a tenant ID for the default tenant; acquire both tokens atomically.
	initial, err := lock.AcquireN(ctx, 2)
	if err != nil {
		return err
	}
	initial.Release() // Returns both tokens, exactly once.

	jobs := []struct {
		tenant string
		tokens int
	}{
		{"A", 2}, {"B", 3}, {"A", 1}, {"B", 2},
	}
	var wg sync.WaitGroup
	// Join started work even if a later acquisition fails.
	defer wg.Wait()
	for _, job := range jobs {
		permit, err := lock.AcquireNFor(ctx, job.tenant, job.tokens)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer permit.Release() // Returns the entire requested weight.
			// Pass ctx to real work separately. Cancelling admission does not
			// stop work or release a successfully granted permit.
			fmt.Printf("tenant %s: working with %d tokens\n", job.tenant, job.tokens)
		}()
	}
	wg.Wait()
	stats := lock.Stats()
	fmt.Printf("finished: %d tokens, %d active permits\n", stats.InUse, stats.ActivePermits)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

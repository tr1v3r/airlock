// Run with: go run ./examples/priority
package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/tr1v3r/airlock"
)

func run() error {
	lock, err := airlock.New(airlock.Config{
		Capacity: 1, MaxQueued: 12, MaxQueuedPerTenant: 8,
		Priority: &airlock.PriorityPolicy{
			Groups: []airlock.PriorityGroup{
				{Name: "high", Turns: 8, MaxQueued: 8},
				{Name: "normal", Turns: 3, MaxQueued: 3},
				{Name: "low", Turns: 1, MaxQueued: 1},
			},
			DefaultGroup: "low",
			TenantGroups: map[string]string{"online": "high", "batch": "normal"},
		},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	var workers sync.WaitGroup
	defer workers.Wait()
	defer cancel() // On an error, cancel pending acquisitions before joining them.

	// Hold all capacity until the demonstration's input queue is established.
	holder, err := lock.Acquire(ctx)
	if err != nil {
		return err
	}
	defer holder.Release()
	type result struct {
		tenant string
		err    error
	}
	results := make(chan result, 12)
	// The default group is low, including an unmapped named tenant (maintenance).
	// Low is queued FIRST, but the scheduler's first cycle is HHHHHHHHMMML.
	tenants := []string{
		"maintenance", "batch", "batch", "batch",
		"online", "online", "online", "online",
		"online", "online", "online", "online",
	}
	for i, tenant := range tenants {
		workers.Add(1)
		go func() {
			defer workers.Done()
			permit, acquireErr := lock.AcquireFor(ctx, tenant)
			// Capacity is one: record the grant before releasing to the next waiter.
			results <- result{tenant: tenant, err: acquireErr}
			if permit != nil {
				permit.Release()
			}
		}()
		// Demo-only barrier: the holder prevents grants, so Queued can only grow.
		// Production callers should block in Acquire, not poll Stats/TryAcquire.
		for lock.Stats().Queued < i+1 {
			if err := ctx.Err(); err != nil {
				return err
			}
			runtime.Gosched()
		}
	}
	holder.Release()
	for range tenants {
		outcome := <-results
		if outcome.err != nil {
			return outcome.err
		}
		fmt.Println(outcome.tenant)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

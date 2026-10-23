// Command tenants demonstrates concurrent work under named-tenant admission.
// Run with: go run ./examples/tenants
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/tr1v3r/airlock"
)

// run demonstrates concurrent work under named-tenant admission.
func run(ctx context.Context) error {
	g, err := airlock.New(airlock.Config{
		Capacity: 2, MaxQueued: 8, MaxQueuedPerTenant: 4,
	})
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	// Join any started work even if a subsequent acquisition fails.
	defer wg.Wait()
	for _, tenant := range []string{"A", "B", "A", "B"} {
		permit, err := g.AcquireFor(ctx, tenant)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer permit.Release()
			// Pass ctx into real work as well: admission cancellation alone
			// does not cancel an already admitted task.
			fmt.Println("working for tenant", tenant)
		}()
	}
	return nil
}

// main runs the tenants admission example.
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

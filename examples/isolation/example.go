// Run with: go run ./examples/isolation
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tr1v3r/airlock"
)

// This synchronous task stops using its resource before returning. A real task
// must likewise cooperate with cancellation before its permit can be released.
func doWork(ctx context.Context) error {
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func runOnline(ctx context.Context, high *airlock.Airlock) error {
	waitCtx, stopWaiting := context.WithTimeout(ctx, 5*time.Millisecond)
	permit, err := high.AcquireFor(waitCtx, "online")
	stopWaiting() // Cancelling acquisition does not revoke a successful permit.
	if err != nil {
		return err // The caller chooses rejection, fallback or a bounded retry.
	}
	defer permit.Release()

	workCtx, stopWork := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopWork()
	return doWork(workCtx)
}

func run() error {
	// A fixed total budget of 12 is partitioned into 4 + 8 at startup.
	// Route authenticated online requests to high and batch/maintenance to low.
	// Never borrow, overflow to the other pool, or add an outer blocking gate.
	high, err := airlock.New(airlock.Config{Capacity: 4, MaxQueued: 16, MaxQueuedPerTenant: 8})
	if err != nil {
		return err
	}
	low, err := airlock.New(airlock.Config{
		Capacity: 8, MaxQueued: 64, MaxQueuedPerTenant: 32,
		Priority: &airlock.PriorityPolicy{
			Groups: []airlock.PriorityGroup{
				{Name: "normal", Turns: 3, MaxQueued: 32},
				{Name: "low", Turns: 1, MaxQueued: 32},
			},
			DefaultGroup: "low",
			TenantGroups: map[string]string{"batch": "normal"},
		},
	})
	if err != nil {
		return err
	}
	// Hold every low-pool token. High can still acquire from its own pool.
	background, err := low.AcquireNFor(context.Background(), "maintenance", 8)
	if err != nil {
		return err
	}
	defer background.Release()
	if err := runOnline(context.Background(), high); err != nil {
		return err
	}
	fmt.Printf("online completed; low still holds %d tokens\n", low.Stats().InUse)
	background.Release()
	fmt.Printf("finished: high=%d low=%d tokens\n", high.Stats().InUse, low.Stats().InUse)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// Run with: go run ./examples/tryacquire
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/tr1v3r/airlock"
)

func run(ctx context.Context) error {
	lock, err := airlock.New(airlock.Config{
		Capacity: 2, MaxQueued: 8, MaxQueuedPerTenant: 4,
	})
	if err != nil {
		return err
	}
	// Background calls may wait in the configured queues when capacity is full.
	background, err := lock.AcquireNFor(ctx, "background", 2)
	if err != nil {
		return err
	}
	defer background.Release()

	// Online calls share the same capacity, but never wait for it or join a queue.
	online, err := lock.TryAcquireFor("online")
	if errors.Is(err, airlock.ErrWouldBlock) {
		fmt.Println("online: busy, use fallback")
	} else if err != nil {
		return err
	} else {
		online.Release()
	}

	background.Release() // Idempotent: the deferred release is safe.
	online, err = lock.TryAcquireNFor("online", 2)
	if err != nil {
		return err
	}
	defer online.Release()
	fmt.Println("online: acquired 2 tokens without waiting for capacity")
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

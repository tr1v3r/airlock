// Command cancellation demonstrates cancellation while waiting without consuming capacity.
// Run with: go run ./examples/cancellation
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tr1v3r/airlock"
)

// main runs the cancellation admission example.
func main() {
	lock, err := airlock.New(airlock.Config{Capacity: 1, MaxQueued: 1, MaxQueuedPerTenant: 1})
	if err != nil {
		log.Fatal(err)
	}
	held, err := lock.AcquireFor(context.Background(), "A")
	if err != nil {
		log.Fatal(err)
	}
	defer held.Release()

	// Capacity is held above, so this acquisition waits until its deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	permit, err := lock.AcquireFor(ctx, "B")
	if err != nil {
		fmt.Println(err)                                      // context deadline exceeded
		fmt.Println("tokens still held:", lock.Stats().InUse) // 1; failure did not take a token.
		return                                                // Do not release a token for the failed acquisition.
	}
	permit.Release()
}

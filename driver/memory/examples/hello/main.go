// This demo uses memory to avoid external services. For ordinary single-process
// admission, use airlock.New instead. See ../lifecycle for cancellable real work.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/driver/memory"
)

// main runs the shared-memory greeting.
func main() {
	if err := hello(); err != nil {
		log.Fatal(err)
	}
}

// hello acquires a shared lease, finishes its work and releases with a live cleanup context.
func hello() error {
	client, err := airlock.NewShared(memory.New(), airlock.SharedOptions{
		Resource: "hello",
		Config: airlock.ResourceConfig{
			Admission: airlock.Config{Capacity: 2},
		},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lease, err := client.Acquire(ctx)
	if err != nil {
		return err
	}
	fmt.Println("hello, shared airlock")
	// This example only prints a greeting. For long-running work, observe Done
	// and renew explicitly as shown in ../lifecycle. Work stops before release.
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer stop()
	return lease.Release(cleanup)
}

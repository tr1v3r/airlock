// Run with: go run ./examples/completion
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/tr1v3r/airlock"
)

func main() {
	lock, err := airlock.New(airlock.Config{Capacity: 2, MaxQueued: 1, MaxQueuedPerTenant: 1})
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make([]int, 3)
	for i := range results {
		permit, err := lock.Acquire(context.Background(), "batch")
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer permit.Release()
			results[i] = i + 10
		}()
	}
	// Completion belongs to the caller. Start waiting after all Add calls.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	fmt.Println("doing other work while the batch finishes")
	<-done
	fmt.Println(results)
}

// Run with: go run ./examples/batch
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/tr1v3r/airlock"
)

func squares(ctx context.Context, inputs []int) ([]int, error) {
	lock, err := airlock.New(airlock.Config{Capacity: 2, MaxQueued: 1, MaxQueuedPerTenant: 1})
	if err != nil {
		return nil, err
	}
	results := make([]int, len(inputs))
	var wg sync.WaitGroup
	// Also join already-started workers if a later acquisition is cancelled.
	defer wg.Wait()
	for i, value := range inputs {
		permit, err := lock.AcquireFor(ctx, "batch")
		if err != nil {
			return nil, err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer permit.Release()
			results[i] = value * value
		}()
	}
	return results, nil
}

func main() {
	results, err := squares(context.Background(), []int{1, 2, 3, 4, 5})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(results)
}

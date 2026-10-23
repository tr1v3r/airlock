// Command hello demonstrates local acquisition and explicit release.
// Run with: go run ./examples/hello
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/tr1v3r/airlock"
)

// main runs the hello admission example.
func main() {
	lock, err := airlock.New(airlock.Config{Capacity: 2})
	if err != nil {
		log.Fatal(err)
	}
	permit, err := lock.Acquire(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer permit.Release()
	fmt.Println("hello, airlock")
}

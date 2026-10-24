package memory_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/driver/memory"
)

func ExampleNew() {
	client, err := airlock.NewShared(memory.New(), airlock.SharedOptions{
		Resource: "workers",
		Config: airlock.ResourceConfig{
			Admission: airlock.Config{Capacity: 4, MaxQueued: 8, MaxQueuedPerTenant: 8},
		},
	})
	if err != nil {
		panic(err)
	}
	lease, err := client.AcquireNFor(context.Background(), "online", 2)
	if err != nil {
		panic(err)
	}
	fmt.Println(lease.Weight(), lease.Valid())
	// Do bounded, cancellable work here; stop if lease.Done closes. Renew explicitly
	// if needed. Release only after the work has actually stopped.
	cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lease.Release(cleanup); err != nil {
		panic(err)
	}
	fmt.Println(lease.Valid())
	// Output:
	// 2 true
	// false
}

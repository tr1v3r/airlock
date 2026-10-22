package memory_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/driver/memory"
)

func ExampleNew() {
	client, err := airlock.NewDistributed(memory.New(), airlock.DistributedOptions{
		Resource:         "workers",
		Config:           airlock.ResourceConfig{Admission: airlock.Config{Capacity: 4, MaxQueued: 8, MaxQueuedPerTenant: 8}, LeaseTTL: time.Second, MaxWait: time.Second, SubmitWindow: time.Second, Retention: time.Minute, MaxRecords: 100},
		OperationTimeout: time.Second, PollInterval: 10 * time.Millisecond,
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
	if err := lease.Release(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println(lease.Valid())
	// Output:
	// 2 true
	// false
}

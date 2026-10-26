package memory_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/driver/memory"
	"github.com/tr1v3r/airlock/protocol"
)

func ExampleNew() {
	client, err := airlock.NewShared(memory.New(), airlock.SharedOptions{
		Resource: "workers",
		Config: protocol.ResourceConfig{
			Admission: protocol.Config{Capacity: 4, MaxQueued: 8, MaxQueuedPerTenant: 8},
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

func ExampleNew_multipleResources() {
	// Both clients use the same driver; Resource selects the capacity pool.
	driver := memory.New()
	newPool := func(resource string, capacity int) (*airlock.Shared, error) {
		return airlock.NewShared(driver, airlock.SharedOptions{
			Resource: resource,
			Config: airlock.ResourceConfig{
				Admission: airlock.Config{
					Capacity: capacity, MaxQueued: 100, MaxQueuedPerTenant: 20,
				},
			},
		})
	}
	exports, err := newPool("export-jobs", 4)
	if err != nil {
		panic(err)
	}
	images, err := newPool("image-jobs", 8)
	if err != nil {
		panic(err)
	}
	release := func(lease *airlock.Lease) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := lease.Release(ctx); err != nil {
			panic(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	exportLease, err := exports.AcquireNFor(ctx, "customer-A", 4)
	if err != nil {
		panic(err)
	}
	defer release(exportLease)
	// The export pool is full, but all image capacity is still available,
	// even when the same tenant name is used in both resources.
	imageLease, err := images.AcquireNFor(ctx, "customer-A", 8)
	if err != nil {
		panic(err)
	}
	defer release(imageLease)
	fmt.Println("export-jobs:", exportLease.Weight())
	fmt.Println("image-jobs:", imageLease.Weight())
	// No business work runs in this example. Real work must stop before release;
	// supervise lease expiry and renew explicitly as in the lifecycle example.
	// Output:
	// export-jobs: 4
	// image-jobs: 8
}

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestIntegrationDistributedClient(t *testing.T) {
	d := integrationDriver(t, integrationDB(t), uniquePrefix())
	client, err := airlock.NewDistributed(d, airlock.DistributedOptions{Resource: "client-lifecycle", Config: airlock.ResourceConfig{Admission: airlock.Config{Capacity: 2, MaxQueued: 4, MaxQueuedPerTenant: 4}, LeaseTTL: 5 * time.Second, MaxWait: time.Second, SubmitWindow: time.Second, Retention: time.Second, MaxRecords: 30}, OperationTimeout: time.Second, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	acquisition, cancel := context.WithCancel(context.Background())
	lease, err := client.AcquireNFor(acquisition, "online", 2)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if !lease.Valid() {
		t.Fatal("acquisition context revoked delivered lease")
	}
	waiting, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = client.AcquireFor(waiting, "batch")
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded wait: %v", err)
	}
	if err := lease.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lease.Done():
	default:
		t.Fatal("released lease still usable")
	}
	replacement, err := client.AcquireN(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

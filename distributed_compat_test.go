package airlock

import (
	"context"
	"errors"
	"testing"
)

func TestDistributedCompatibility(t *testing.T) {
	var options DistributedOptions = clientOptions() //nolint:staticcheck // ST1023: assert source compatibility between the exported option types.
	if _, err := NewDistributed(nil, options); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("legacy constructor validation: %v", err)
	}
	driver := &clientStateDriver{}
	legacy, err := NewDistributed(driver, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(driver.commands()) != 0 {
		t.Fatal("construction accessed the backend")
	}
	// Both exported names identify the same type and share one lease lifecycle.
	var current *Shared = legacy //nolint:staticcheck // ST1023: assert source compatibility between the exported client types.
	lease, err := current.AcquireN(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Weight() != 2 || !lease.Valid() {
		t.Fatal("legacy constructor did not grant the full allocation")
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lease.Valid() {
		t.Fatal("released lease remains valid")
	}
}

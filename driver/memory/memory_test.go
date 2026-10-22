package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
	"github.com/tr1v3r/airlock/internal/driverconformance"
)

func TestConformance(t *testing.T) {
	driverconformance.Run(t, func(*testing.T) airlock.Driver { return New() })
}
func TestCancellation(t *testing.T) {
	d := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Submit(ctx, airlock.Request{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	d.resources = map[string]*resource{"locked": {gate: make(chan struct{}, 1)}}
	r := d.resources["locked"]
	r.gate <- struct{}{}
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := d.Submit(ctx, memoryRequest("locked")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	<-r.gate
}
func TestInvalidRequestDoesNotPersist(t *testing.T) {
	d := New()
	if _, err := d.Submit(context.Background(), airlock.Request{}); err == nil {
		t.Fatal("invalid accepted")
	}
	if d.resources[""] != nil {
		t.Fatal("invalid request persisted")
	}
}

func memoryRequest(resource string) airlock.Request {
	now := time.Now()
	r := airlock.NewRequest(now.Add(time.Second))
	r.Resource = resource
	r.WaitUntil = now.Add(time.Second).UTC().Truncate(time.Millisecond)
	r.Weight = 1
	r.Config = airlock.ResourceConfig{Admission: airlock.Config{Capacity: 1}, LeaseTTL: time.Second, MaxWait: time.Second, SubmitWindow: time.Second, Retention: time.Second, MaxRecords: 10}
	return r
}

// stagedCancellation models cancellation after lock acquisition or after commit.
type stagedCancellation struct {
	context.Context
	calls, at int
}

func (c *stagedCancellation) Err() error {
	c.calls++
	if c.calls >= c.at {
		return context.Canceled
	}
	return nil
}
func TestCancellationAtAtomicBoundary(t *testing.T) {
	for _, at := range []int{2, 3} {
		d := New()
		ctx := &stagedCancellation{Context: context.Background(), at: at}
		request := memoryRequest("cancel-boundary")
		if _, err := d.Submit(ctx, request); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		stored := d.resources[request.Resource].snapshot
		if (len(stored) > 0) != (at == 3) {
			t.Fatalf("commit state at boundary %d", at)
		}
	}
}

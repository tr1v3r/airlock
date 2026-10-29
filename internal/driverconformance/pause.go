package driverconformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

// pauseResume exercises persisted pauses through every driver's public protocol.
func pauseResume(t *testing.T, driver airlock.Driver) {
	ctx := context.Background()
	cfg := airlock.ResourceConfig{Admission: airlock.Config{Capacity: 4, MaxQueued: 3, MaxQueuedPerTenant: 2}, LeaseTTL: 30 * time.Second, MaxWait: 30 * time.Second, SubmitWindow: 30 * time.Second, Retention: time.Minute, MaxRecords: 64}
	h := &harness{t: t, driver: driver, resource: "pause-" + airlock.NewRequest(time.Now()).ID, cfg: cfg}
	client, err := airlock.NewShared(driver, airlock.SharedOptions{Resource: h.resource, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	holder, head, tail, large := h.request("holder", 3), h.request("head", 3), h.request("tail", 1), h.request("large", 100)
	grant := h.must(airlock.Submit, holder, "", airlock.Granted)
	h.must(airlock.Submit, head, "", airlock.Queued)
	paused, err := client.Resize(ctx, 0, 1)
	if err != nil || paused.Capacity != 0 || paused.Used != 3 || paused.Queued != 1 || paused.CapacityVersion != 2 {
		t.Fatal(paused, err)
	}
	if status, err := client.Inspect(ctx); err != nil || status != paused {
		t.Fatal(status, err)
	}
	if status, err := client.Resize(ctx, 0, 2); err != nil || status.CapacityVersion != 2 {
		t.Fatal(status, err)
	}
	if _, err := client.Resize(ctx, -1, 2); !errors.Is(err, airlock.ErrInvalidConfig) {
		t.Fatal(err)
	}
	h.must(airlock.Renew, holder, grant.LeaseID, airlock.Granted)
	h.must(airlock.Submit, tail, "", airlock.Queued)
	h.must(airlock.Submit, large, "", airlock.Queued)
	h.wantError(airlock.Submit, h.request("full", 1), "", airlock.ErrQueueFull)
	h.must(airlock.Observe, head, "", airlock.Queued)
	if _, err := client.Resize(ctx, 4, 2); err != nil {
		t.Fatal(err)
	}
	h.wantError(airlock.Observe, large, "", airlock.ErrWeightExceedsCapacity)
	h.must(airlock.Observe, tail, "", airlock.Queued) // protected head survives pause
	h.must(airlock.Release, holder, grant.LeaseID, airlock.Released)
	hg := h.must(airlock.Observe, head, "", airlock.Granted)
	tg := h.must(airlock.Observe, tail, "", airlock.Granted)
	if _, err := client.Resize(ctx, 0, 3); err != nil {
		t.Fatal(err)
	}
	h.must(airlock.Release, head, hg.LeaseID, airlock.Released)
	h.must(airlock.Release, tail, tg.LeaseID, airlock.Released)
	pending := h.request("pending", 100)
	h.must(airlock.Submit, pending, "", airlock.Queued)
	h.must(airlock.Cancel, pending, "", airlock.Cancelled)
	if s, e := client.Inspect(ctx); e != nil || s.Capacity != 0 || s.Used != 0 || s.Queued != 0 {
		t.Fatal(s, e)
	}
	// A new Shared client must use persisted zero, not its positive creation hint.
	reopened, e := airlock.NewShared(driver, airlock.SharedOptions{Resource: h.resource, Config: cfg})
	if e != nil {
		t.Fatal(e)
	}
	waiting, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		permit, err := reopened.AcquireN(waiting, 100)
		if permit != nil {
			_ = permit.Release(ctx)
		}
		done <- err
	}()
	for {
		status, err := reopened.Inspect(waiting)
		if err != nil {
			t.Fatal(err)
		}
		if status.Capacity != 0 || status.Used != 0 {
			t.Fatal(status)
		}
		if status.Queued == 1 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("paused acquisition returned before queueing: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
	if _, e := reopened.Resize(ctx, 2, 4); e != nil {
		t.Fatal(e)
	}
	if err := <-done; !errors.Is(err, airlock.ErrWeightExceedsCapacity) {
		t.Fatal(err)
	}
}

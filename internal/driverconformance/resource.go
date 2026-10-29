package driverconformance

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

// RunResourceManagement checks dynamic capacity, recovery and management races
// against the same public contract on volatile and real storage backends.
func RunResourceManagement(t *testing.T, driver airlock.Driver) {
	t.Helper()
	manager, ok := driver.(airlock.ResourceManager)
	if !ok {
		t.Fatal("driver lacks resource management")
	}
	t.Run("native_int_capacity", func(t *testing.T) { nativeCapacity(t, driver) })
	t.Run("pause_resume", func(t *testing.T) { pauseResume(t, driver) })
	ctx := context.Background()
	name := "resize-" + airlock.NewRequest(time.Now()).Owner
	if _, err := manager.InspectResource(ctx, name); !errors.Is(err, airlock.ErrResourceNotFound) {
		t.Fatalf("missing Inspect: %v", err)
	}
	if _, err := manager.Resize(ctx, airlock.ResizeRequest{Resource: name, Capacity: 2, ExpectedVersion: 1}); !errors.Is(err, airlock.ErrResourceNotFound) {
		t.Fatalf("missing Resize: %v", err)
	}
	cfg := airlock.ResourceConfig{Admission: airlock.Config{Capacity: 3, MaxQueued: 8, MaxQueuedPerTenant: 8}, LeaseTTL: 30 * time.Second, MaxWait: 30 * time.Second, SubmitWindow: 30 * time.Second, Retention: time.Minute, MaxRecords: 64}
	request := func(tenant string, weight int) airlock.Request {
		now := time.Now()
		r := airlock.NewRequest(now.Add(30 * time.Second))
		r.Resource, r.Tenant, r.Weight, r.Config, r.WaitUntil = name, tenant, weight, cfg, r.SubmitUntil
		return r
	}
	holder, heavy, light := request("holder", 3), request("heavy", 3), request("light", 1)
	grant, err := driver.Submit(ctx, holder)
	if err != nil || grant.Status != airlock.Granted {
		t.Fatal(grant, err)
	}
	for _, r := range []airlock.Request{heavy, light} {
		if out, err := driver.Submit(ctx, r); err != nil || out.Status != airlock.Queued {
			t.Fatal(out, err)
		}
	}
	before, err := manager.InspectResource(ctx, name)
	if err != nil || before.Capacity != 3 || before.Used != 3 || before.Queued != 2 || before.CapacityVersion != 1 {
		t.Fatal(before, err)
	}
	after, err := manager.Resize(ctx, airlock.ResizeRequest{Resource: name, Capacity: 2, ExpectedVersion: 1})
	if err != nil || after.Capacity != 2 || after.Used != 3 || after.Queued != 1 || after.CapacityVersion != 2 {
		t.Fatal(after, err)
	}
	if result, err := driver.Observe(ctx, heavy); !errors.Is(err, airlock.ErrWeightExceedsCapacity) || result.Status != airlock.Rejected {
		t.Fatal(result, err)
	}
	if _, err := driver.Renew(ctx, airlock.LeaseRef{Request: holder, ID: grant.LeaseID}); err != nil {
		t.Fatal(err)
	}
	if result, err := driver.Observe(ctx, light); err != nil || result.Status != airlock.Queued {
		t.Fatal(result, err)
	}
	if _, err := driver.Release(ctx, airlock.LeaseRef{Request: holder, ID: grant.LeaseID}); err != nil {
		t.Fatal(err)
	}
	if result, err := driver.Observe(ctx, light); err != nil || result.Status != airlock.Granted {
		t.Fatal(result, err)
	}
	// The creation hint is not a policy conflict or a ceiling for later Submit.
	large := request("large", 4)
	large.Config.Admission.Capacity = 1
	if _, err := manager.Resize(ctx, airlock.ResizeRequest{Resource: name, Capacity: 5, ExpectedVersion: 2}); err != nil {
		t.Fatal(err)
	}
	out, err := driver.Submit(ctx, large)
	if err != nil || out.Status != airlock.Granted {
		t.Fatal(out, err)
	}
	large.Config.Admission.Capacity = 9
	if retry, err := driver.Submit(ctx, large); err != nil || retry.LeaseID != out.LeaseID {
		t.Fatal(retry, err)
	}
	// Reusing the same expected version permits exactly one capacity decision.
	var wg sync.WaitGroup
	errorsCh := make(chan error, 2)
	for _, capacity := range []int{6, 7} {
		wg.Add(1)
		go func(capacity int) {
			defer wg.Done()
			_, err := manager.Resize(ctx, airlock.ResizeRequest{Resource: name, Capacity: capacity, ExpectedVersion: 3})
			errorsCh <- err
		}(capacity)
	}
	wg.Wait()
	close(errorsCh)
	success, conflict := 0, 0
	for err := range errorsCh {
		if err == nil {
			success++
		} else if errors.Is(err, airlock.ErrCapacityVersionConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("resize race: success=%d conflict=%d", success, conflict)
	}
	final, err := manager.InspectResource(ctx, name)
	if err != nil || final.CapacityVersion != 4 || final.Used != 5 {
		t.Fatal(final, err)
	}
	// Inspect is non-mutating, and same-capacity management does not bump its version.
	again, err := manager.InspectResource(ctx, name)
	if err != nil || again != final {
		t.Fatal(again, err)
	}
	noop, err := manager.Resize(ctx, airlock.ResizeRequest{Resource: name, Capacity: final.Capacity, ExpectedVersion: 4})
	if err != nil || noop.CapacityVersion != 4 || noop.Revision <= final.Revision {
		t.Fatal(noop, err)
	}
	isolated, err := manager.InspectResource(ctx, name+"-other")
	if !errors.Is(err, airlock.ErrResourceNotFound) || isolated != (airlock.ResourceStatus{}) {
		t.Fatal(isolated, err)
	}
	client, err := airlock.NewShared(driver, airlock.SharedOptions{Resource: name, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.Inspect(ctx)
	if err != nil || status.CapacityVersion != 4 {
		t.Fatal(status, err)
	}
	if _, err := client.Resize(ctx, 8, 4); err != nil {
		t.Fatal(err)
	}
}

// nativeCapacity verifies that weights are amounts, not record counts. The odd
// native maximum also detects accidental floating-point conversion in Redis Lua.
func nativeCapacity(t *testing.T, driver airlock.Driver) {
	t.Helper()
	manager := driver.(airlock.ResourceManager)
	ctx := context.Background()
	maximum := int(^uint(0) >> 1)
	name := "native-" + airlock.NewRequest(time.Now()).Owner
	cfg := airlock.ResourceConfig{Admission: airlock.Config{Capacity: maximum, MaxQueued: 8, MaxQueuedPerTenant: 8}, LeaseTTL: 30 * time.Second, MaxWait: 30 * time.Second, SubmitWindow: 30 * time.Second, Retention: time.Minute, MaxRecords: 64}
	request := func(weight int) airlock.Request {
		r := airlock.NewRequest(time.Now().Add(30 * time.Second))
		r.Resource, r.Weight, r.Config, r.WaitUntil = name, weight, cfg, r.SubmitUntil
		return r
	}
	large, small := request(maximum-1), request(1)
	first, err := driver.Submit(ctx, large)
	if err != nil || first.Status != airlock.Granted {
		t.Fatal(first, err)
	}
	second, err := driver.Submit(ctx, small)
	if err != nil || second.Status != airlock.Granted {
		t.Fatal(second, err)
	}
	status, err := manager.InspectResource(ctx, name)
	if err != nil || status.Capacity != maximum || status.Used != maximum {
		t.Fatal(status, err)
	}
	status, err = manager.Resize(ctx, airlock.ResizeRequest{Resource: name, Capacity: 1, ExpectedVersion: 1})
	if err != nil || status.Used != maximum {
		t.Fatal(status, err)
	}
	if _, err := driver.Renew(ctx, airlock.LeaseRef{Request: large, ID: first.LeaseID}); err != nil {
		t.Fatal(err)
	}
	for _, lease := range []airlock.LeaseRef{{Request: large, ID: first.LeaseID}, {Request: small, ID: second.LeaseID}} {
		if _, err := driver.Release(ctx, lease); err != nil {
			t.Fatal(err)
		}
	}
	cfg.Admission.Capacity = 1
	client, err := airlock.NewShared(driver, airlock.SharedOptions{Resource: name, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resize(ctx, maximum, 2); err != nil {
		t.Fatal(err)
	}
	lease, err := client.AcquireN(ctx, maximum)
	if err != nil || lease.Weight() != maximum {
		t.Fatal(lease, err)
	}
	if got, err := client.Inspect(ctx); err != nil || got.Used != maximum {
		t.Fatal(got, err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := client.Inspect(ctx); err != nil || got.Used != 0 {
		t.Fatal(got, err)
	}
}

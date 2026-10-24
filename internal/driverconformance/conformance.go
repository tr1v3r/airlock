// Package driverconformance supplies backend-independent protocol tests. It is
// test infrastructure: production packages must not import it. Each test obtains
// a new driver and a unique resource, so a factory may share a real backend.
package driverconformance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

// Run verifies the durable admission contract against factory. The factory is
// called independently for each subtest, owns connection cleanup through t, and
// must return a driver connected to an available backend. These tests deliberately
// use only public protocol operations, never backend-specific state inspection.
func Run(t *testing.T, factory func(*testing.T) airlock.Driver) {
	t.Helper()
	tests := []struct {
		name string
		run  func(*harness)
	}{
		{"idempotency_and_conflicts", idempotency},
		{"weight_validation", weightValidation},
		{"cancel_before_submit", cancelBeforeSubmit},
		{"whole_weight_and_protected_head", protectedHead},
		{"queue_bounds_and_stable_rejection", queueBounds},
		{"group_and_tenant_queue_bounds", groupBounds},
		{"priority_and_tenant_rotation", priorityRotation},
		{"tenant_rotation_after_cancel", tenantRotation},
		{"selected_cancel_consumes_turn", selectedCancel},
		{"queue_expiry", queueExpiry},
		{"lease_expiry_and_renew", leaseExpiry},
		{"live_history_and_terminal_gc", historyGC},
		{"concurrent_weighted_admission", concurrentAdmission},
		{"concurrent_same_identity_and_release", concurrentIdentity},
		{"concurrent_owner_conflicts", concurrentOwnerConflict},
		{"concurrent_cancel_and_submit", concurrentCancellation},
		{"concurrent_renew_and_release", concurrentRenewRelease},
		{"pre_cancelled_operations_do_not_mutate", cancelledOperations},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := airlock.ResourceConfig{
				Admission: airlock.Config{Capacity: 3, MaxQueued: 64, MaxQueuedPerTenant: 64},
				LeaseTTL:  30 * time.Second, MaxWait: 30 * time.Second,
				SubmitWindow: 30 * time.Second, Retention: 100 * time.Millisecond, MaxRecords: 256,
			}
			identity := airlock.NewRequest(time.Now().Add(time.Minute))
			test.run(&harness{t: t, driver: factory(t), resource: "conformance-" + identity.ID, cfg: cfg})
		})
	}
}

// harness binds one backend, isolated resource and protocol configuration.
type harness struct {
	// t owns assertions and backend cleanup for this isolated subtest.
	t *testing.T
	// driver is the backend under conformance testing.
	driver airlock.Driver
	// resource uniquely isolates this subtest from other backend users.
	resource string
	// cfg supplies the immutable policy used for requests in this subtest.
	cfg airlock.ResourceConfig
}

// request creates a fresh identity with deadlines inside the configured backend windows.
func (h *harness) request(tenant string, weight int) airlock.Request {
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := airlock.NewRequest(now.Add(h.cfg.SubmitWindow - time.Millisecond))
	r.Resource, r.Tenant, r.Weight, r.Config = h.resource, tenant, weight, h.cfg
	r.WaitUntil = now.Add(h.cfg.MaxWait - time.Millisecond)
	return r
}

// execute invokes one bounded backend operation, allowing a larger bound in stress mode.
func (h *harness) execute(kind airlock.Operation, request airlock.Request, lease string) (airlock.Result, error) {
	timeout := 5 * time.Second
	if os.Getenv("AIRLOCK_STRESS") == "1" {
		// Race instrumentation and CPU=1 amplify snapshot/CAS contention.
		// Keep the stress run bounded without turning this into a latency SLA.
		timeout = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return Invoke(ctx, h.driver, airlock.Command{Kind: kind, Request: request, LeaseID: lease})
}

// must requires the expected successful status and validates grant accounting.
func (h *harness) must(kind airlock.Operation, request airlock.Request, lease string, status airlock.Status) airlock.Result {
	h.t.Helper()
	result, err := h.execute(kind, request, lease)
	if err != nil || result.Status != status {
		h.t.Fatalf("%s tenant=%q: result=%+v err=%v; want %s", kind, request.Tenant, result, err, status)
	}
	if result.Weight != request.Weight || result.Revision == 0 {
		h.t.Fatalf("%s invalid result accounting: %+v", kind, result)
	}
	if status == airlock.Granted && (result.LeaseID == "" || result.Remaining <= 0 || result.Remaining > request.Config.LeaseTTL) {
		h.t.Fatalf("invalid grant: %+v", result)
	}
	return result
}

// wantError requires a specific error identity while preserving the returned protocol result.
func (h *harness) wantError(kind airlock.Operation, request airlock.Request, lease string, want error) airlock.Result {
	h.t.Helper()
	result, err := h.execute(kind, request, lease)
	if !errors.Is(err, want) {
		h.t.Fatalf("%s tenant=%q: result=%+v err=%v; want %v", kind, request.Tenant, result, err, want)
	}
	return result
}

// idempotency verifies stable grants, immutable request identity and at-most-once release accounting.
func idempotency(h *harness) {
	r := h.request("a", 2)
	first := h.must(airlock.Submit, r, "", airlock.Granted)
	retry := h.must(airlock.Submit, r, "", airlock.Granted)
	if first.LeaseID != retry.LeaseID || retry.Remaining > first.Remaining || retry.Revision <= first.Revision {
		h.t.Fatalf("retry changed grant identity/TTL/revision: first=%+v retry=%+v", first, retry)
	}
	for _, change := range []func(*airlock.Request){
		func(r *airlock.Request) { r.Owner = airlock.NewRequest(time.Now()).Owner },
		func(r *airlock.Request) { r.Weight = 1 },
		func(r *airlock.Request) { r.Tenant = "other" },
		func(r *airlock.Request) { r.WaitUntil = r.WaitUntil.Add(-time.Millisecond) },
	} {
		conflict := r
		change(&conflict)
		h.wantError(airlock.Submit, conflict, "", airlock.ErrRequestConflict)
	}
	conflict := r
	conflict.Config.Admission.Capacity++
	h.wantError(airlock.Submit, conflict, "", airlock.ErrConfigConflict)
	h.wantError(airlock.Renew, r, "incorrect-lease", airlock.ErrLeaseLost)
	h.wantError(airlock.Release, r, "incorrect-lease", airlock.ErrLeaseLost)
	h.must(airlock.Observe, r, "", airlock.Granted)
	other := h.request("b", 2)
	h.must(airlock.Submit, other, "", airlock.Queued)
	h.must(airlock.Release, r, first.LeaseID, airlock.Released)
	h.must(airlock.Release, r, first.LeaseID, airlock.Released)
	h.must(airlock.Submit, r, "", airlock.Released)
	h.wantError(airlock.Renew, r, first.LeaseID, airlock.ErrLeaseLost)
	granted := h.must(airlock.Observe, other, "", airlock.Granted)
	third := h.request("c", 2)
	h.must(airlock.Submit, third, "", airlock.Queued)
	h.must(airlock.Release, other, granted.LeaseID, airlock.Released)
	h.must(airlock.Observe, third, "", airlock.Granted)
}

// cancelBeforeSubmit verifies tombstones prevent late submissions from acquiring capacity.
func cancelBeforeSubmit(h *harness) {
	r := h.request("a", 3)
	h.must(airlock.Cancel, r, "", airlock.Cancelled)
	h.must(airlock.Submit, r, "", airlock.Cancelled)
	h.must(airlock.Cancel, r, "", airlock.Cancelled)
	h.must(airlock.Observe, r, "", airlock.Cancelled)
	fresh := h.request("b", 3)
	h.must(airlock.Submit, fresh, "", airlock.Granted)
	h.must(airlock.Cancel, fresh, "", airlock.Cancelled)
	h.must(airlock.Submit, fresh, "", airlock.Cancelled)
	h.must(airlock.Submit, h.request("c", 3), "", airlock.Granted)
}

// weightValidation checks invalid weights leave full capacity available.
func weightValidation(h *harness) {
	for _, weight := range []int{-1, 0, 4} {
		want := airlock.ErrInvalidWeight
		if weight > h.cfg.Admission.Capacity {
			want = airlock.ErrWeightExceedsCapacity
		}
		h.wantError(airlock.Submit, h.request("a", weight), "", want)
	}
	h.must(airlock.Submit, h.request("b", h.cfg.Admission.Capacity), "", airlock.Granted)
}

// protectedHead verifies small requests cannot bypass a selected whole-weight request.
func protectedHead(h *harness) {
	holder := h.request("holder", 2)
	grant := h.must(airlock.Submit, holder, "", airlock.Granted)
	large, small := h.request("large", 3), h.request("small", 1)
	h.must(airlock.Submit, large, "", airlock.Queued)
	h.must(airlock.Submit, small, "", airlock.Queued)
	for i := 0; i < 3; i++ {
		h.must(airlock.Observe, small, "", airlock.Queued)
	}
	h.must(airlock.Release, holder, grant.LeaseID, airlock.Released)
	h.must(airlock.Observe, small, "", airlock.Queued)
	grant = h.must(airlock.Observe, large, "", airlock.Granted)
	h.must(airlock.Release, large, grant.LeaseID, airlock.Released)
	h.must(airlock.Observe, small, "", airlock.Granted)
}

// queueBounds checks global/tenant limits and retained rejection outcomes.
func queueBounds(h *harness) {
	h.cfg.Admission.MaxQueued = 2
	h.cfg.Admission.MaxQueuedPerTenant = 1
	holder := h.request("holder", 3)
	h.must(airlock.Submit, holder, "", airlock.Granted)
	a := h.request("a", 1)
	h.must(airlock.Submit, a, "", airlock.Queued)
	h.wantError(airlock.Submit, h.request("a", 1), "", airlock.ErrTenantQueueFull)
	h.must(airlock.Submit, h.request("b", 1), "", airlock.Queued)
	rejected := h.request("c", 1)
	if got := h.wantError(airlock.Submit, rejected, "", airlock.ErrQueueFull); got.Status != airlock.Rejected {
		h.t.Fatalf("queue rejection status=%s", got.Status)
	}
	h.must(airlock.Cancel, a, "", airlock.Cancelled)
	h.wantError(airlock.Submit, rejected, "", airlock.ErrQueueFull)
	h.must(airlock.Submit, h.request("c", 1), "", airlock.Queued)
}

// groupBounds verifies reserved group waiting budgets and per-tenant bounds.
func groupBounds(h *harness) {
	h.cfg.Admission.MaxQueued = 4
	h.cfg.Admission.MaxQueuedPerTenant = 1
	h.cfg.Admission.Priority = &airlock.PriorityPolicy{
		Groups:       []airlock.PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 2}, {Name: "low", Turns: 1, MaxQueued: 2}},
		DefaultGroup: "low", TenantGroups: map[string]string{"h1": "high", "h2": "high", "h3": "high"},
	}
	h.must(airlock.Submit, h.request("holder", 3), "", airlock.Granted)
	a := h.request("h1", 1)
	h.must(airlock.Submit, a, "", airlock.Queued)
	h.wantError(airlock.Submit, h.request("h1", 1), "", airlock.ErrTenantQueueFull)
	h.must(airlock.Submit, h.request("h2", 1), "", airlock.Queued)
	h.wantError(airlock.Submit, h.request("h3", 1), "", airlock.ErrGroupQueueFull)
	h.must(airlock.Submit, h.request("l1", 1), "", airlock.Queued)
	h.must(airlock.Cancel, a, "", airlock.Cancelled)
	h.must(airlock.Submit, h.request("h3", 1), "", airlock.Queued)
}

// priorityRotation verifies weighted group turns are independent of observation frequency.
func priorityRotation(h *harness) {
	h.cfg.Admission.Capacity = 1
	h.cfg.Admission.Priority = &airlock.PriorityPolicy{
		Groups:       []airlock.PriorityGroup{{Name: "high", Turns: 8, MaxQueued: 24}, {Name: "normal", Turns: 3, MaxQueued: 12}, {Name: "low", Turns: 1, MaxQueued: 8}},
		DefaultGroup: "low", TenantGroups: map[string]string{"h1": "high", "h2": "high", "m": "normal"},
	}
	holder := h.request("holder", 1)
	grant := h.must(airlock.Submit, holder, "", airlock.Granted)
	queues := map[string][]airlock.Request{}
	for _, spec := range []struct {
		tenant string
		count  int
	}{{"l", 2}, {"m", 6}, {"h1", 8}, {"h2", 8}} {
		for i := 0; i < spec.count; i++ {
			r := h.request(spec.tenant, 1)
			queues[spec.tenant] = append(queues[spec.tenant], r)
			h.must(airlock.Submit, r, "", airlock.Queued)
		}
	}
	h.must(airlock.Release, holder, grant.LeaseID, airlock.Released)
	for cycle := 0; cycle < 2; cycle++ {
		for _, tenant := range []string{"h1", "h2", "h1", "h2", "h1", "h2", "h1", "h2", "m", "m", "m", "l"} {
			r := queues[tenant][0]
			// Poll all other tenants first. Observation frequency cannot change turns.
			for _, other := range []string{"l", "m", "h2", "h1"} {
				if other != tenant && len(queues[other]) > 0 {
					h.must(airlock.Observe, queues[other][0], "", airlock.Queued)
				}
			}
			g := h.must(airlock.Observe, r, "", airlock.Granted)
			h.must(airlock.Release, r, g.LeaseID, airlock.Released)
			queues[tenant] = queues[tenant][1:]
		}
	}
}

// tenantRotation checks FIFO round-robin order after a tenant leaves and rejoins.
func tenantRotation(h *harness) {
	h.cfg.Admission.Capacity = 1
	holder := h.request("holder", 1)
	g := h.must(airlock.Submit, holder, "", airlock.Granted)
	a1, a2, b, c := h.request("a", 1), h.request("a", 1), h.request("b", 1), h.request("c", 1)
	for _, r := range []airlock.Request{a1, a2, b, c} {
		h.must(airlock.Submit, r, "", airlock.Queued)
	}
	h.must(airlock.Cancel, b, "", airlock.Cancelled)
	b2 := h.request("b", 1)
	h.must(airlock.Submit, b2, "", airlock.Queued)
	h.must(airlock.Release, holder, g.LeaseID, airlock.Released)
	for _, r := range []airlock.Request{a1, c, b2, a2} {
		g = h.must(airlock.Observe, r, "", airlock.Granted)
		h.must(airlock.Release, r, g.LeaseID, airlock.Released)
	}
}

// selectedCancel verifies cancellation does not refund a consumed group selection turn.
func selectedCancel(h *harness) {
	h.cfg.Admission.Priority = &airlock.PriorityPolicy{
		Groups:       []airlock.PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 8}, {Name: "low", Turns: 1, MaxQueued: 8}},
		DefaultGroup: "low", TenantGroups: map[string]string{"h": "high"},
	}
	holder := h.request("holder", 2)
	g := h.must(airlock.Submit, holder, "", airlock.Granted)
	h1, h2, h3, low := h.request("h", 3), h.request("h", 3), h.request("h", 3), h.request("low", 3)
	for _, r := range []airlock.Request{h1, h2, h3, low} {
		h.must(airlock.Submit, r, "", airlock.Queued)
	}
	h.must(airlock.Cancel, h1, "", airlock.Cancelled)
	h.must(airlock.Release, holder, g.LeaseID, airlock.Released)
	for _, r := range []airlock.Request{h2, low, h3} {
		g = h.must(airlock.Observe, r, "", airlock.Granted)
		h.must(airlock.Release, r, g.LeaseID, airlock.Released)
	}
}

// queueExpiry checks deadline expiry frees waiting space without reviving the request.
func queueExpiry(h *harness) {
	h.cfg.MaxWait = 300 * time.Millisecond
	h.cfg.Admission.MaxQueued = 1
	h.cfg.Admission.MaxQueuedPerTenant = 1
	holder := h.request("holder", 3)
	h.must(airlock.Submit, holder, "", airlock.Granted)
	queued := h.request("a", 3)
	h.must(airlock.Submit, queued, "", airlock.Queued)
	sleepUntil(queued.WaitUntil.Add(30 * time.Millisecond))
	h.must(airlock.Observe, queued, "", airlock.Expired)
	h.must(airlock.Submit, queued, "", airlock.Expired)
	h.must(airlock.Submit, h.request("b", 3), "", airlock.Queued)
}

// leaseExpiry verifies renewal identity, capacity reclamation and terminal release behavior.
func leaseExpiry(h *harness) {
	h.cfg.LeaseTTL = 300 * time.Millisecond
	r := h.request("a", 3)
	g := h.must(airlock.Submit, r, "", airlock.Granted)
	renewed := h.must(airlock.Renew, r, g.LeaseID, airlock.Granted)
	start := time.Now()
	if renewed.LeaseID != g.LeaseID {
		h.t.Fatal("renew changed lease ID")
	}
	waiting := h.request("b", 3)
	h.must(airlock.Submit, waiting, "", airlock.Queued)
	sleepUntil(start.Add(h.cfg.LeaseTTL + 100*time.Millisecond))
	h.must(airlock.Observe, waiting, "", airlock.Granted)
	h.must(airlock.Observe, r, "", airlock.Expired)
	h.wantError(airlock.Renew, r, g.LeaseID, airlock.ErrLeaseLost)
	h.must(airlock.Release, r, g.LeaseID, airlock.Expired)
	h.must(airlock.Observe, waiting, "", airlock.Granted)
}

// historyGC verifies live records survive GC and terminal retention prevents premature ID reuse.
func historyGC(h *harness) {
	h.cfg.MaxRecords = 1
	h.cfg.SubmitWindow = 300 * time.Millisecond
	r := h.request("a", 3)
	g := h.must(airlock.Submit, r, "", airlock.Granted)
	sleepUntil(r.SubmitUntil.Add(30 * time.Millisecond))
	// Submission cutoff and the recovery window never collect a live lease.
	h.must(airlock.Renew, r, g.LeaseID, airlock.Granted)
	h.must(airlock.Submit, r, "", airlock.Granted)
	h.wantError(airlock.Submit, h.request("b", 1), "", airlock.ErrHistoryFull)
	h.must(airlock.Release, r, g.LeaseID, airlock.Released)
	h.wantError(airlock.Submit, h.request("b", 1), "", airlock.ErrHistoryFull)
	time.Sleep(h.cfg.Retention + 30*time.Millisecond)
	h.must(airlock.Submit, h.request("b", 3), "", airlock.Granted)
	h.wantError(airlock.Submit, r, "", airlock.ErrSubmitExpired)
}

// sleepUntil waits only when a conformance deadline is still in the future.
func sleepUntil(deadline time.Time) {
	if delay := time.Until(deadline); delay > 0 {
		time.Sleep(delay)
	}
}

// concurrentAdmission checks weighted capacity and eventual completion across concurrent tenants.
func concurrentAdmission(h *harness) {
	h.cfg.Admission.Capacity = 7
	const clients = 24
	requests := make([]airlock.Request, clients)
	for i := range requests {
		requests[i] = h.request(fmt.Sprintf("tenant-%d", i%4), 1+i%3)
	}
	var inUse atomic.Int64
	var completed atomic.Int64
	var workers sync.WaitGroup
	start := make(chan struct{})
	allSubmitted := make(chan struct{})
	var submitted sync.WaitGroup
	submitted.Add(clients)
	for _, request := range requests {
		workers.Add(1)
		go func(r airlock.Request) {
			defer workers.Done()
			<-start
			result, err := h.execute(airlock.Submit, r, "")
			if err == nil && result.Status == airlock.Granted {
				if used := inUse.Add(int64(r.Weight)); used > int64(h.cfg.Admission.Capacity) {
					h.t.Errorf("concurrent initial grants own %d > %d", used, h.cfg.Admission.Capacity)
				}
			}
			submitted.Done()
			<-allSubmitted // No release until every initial submission settles.
			if err != nil {
				h.t.Errorf("concurrent submit: %v", err)
				return
			}
			deadline := time.Now().Add(20 * time.Second)
			for result.Status == airlock.Queued && time.Now().Before(deadline) {
				// Observe may atomically rewrite a bounded resource snapshot.
				// Avoid a 24-client polling storm under race/coverage instruments;
				// this tests capacity and completion, not an operation-latency SLA.
				time.Sleep(50 * time.Millisecond)
				result, err = h.execute(airlock.Observe, r, "")
				if err != nil {
					h.t.Errorf("concurrent observe: %v", err)
					return
				}
				if result.Status == airlock.Granted {
					if used := inUse.Add(int64(r.Weight)); used > int64(h.cfg.Admission.Capacity) {
						h.t.Errorf("concurrent grants own %d > %d", used, h.cfg.Admission.Capacity)
					}
				}
			}
			if result.Status != airlock.Granted || result.Weight != r.Weight || result.LeaseID == "" {
				h.t.Errorf("concurrent grant invalid: %+v", result)
				return
			}
			// Stop simulated work before release can grant the next request.
			inUse.Add(-int64(r.Weight))
			for i := 0; i < 2; i++ {
				released, releaseErr := h.execute(airlock.Release, r, result.LeaseID)
				if releaseErr != nil || released.Status != airlock.Released {
					h.t.Errorf("concurrent release %d: %+v %v", i, released, releaseErr)
					return
				}
			}
			completed.Add(1)
		}(request)
	}
	close(start)
	submitted.Wait()
	close(allSubmitted)
	workers.Wait()
	if completed.Load() != clients || inUse.Load() != 0 {
		h.t.Fatalf("completed=%d want=%d; inUse=%d", completed.Load(), clients, inUse.Load())
	}
}

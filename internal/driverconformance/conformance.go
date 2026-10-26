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

	"github.com/tr1v3r/airlock/protocol"
)

// Run verifies the durable admission contract against factory. The factory is
// called independently for each subtest, owns connection cleanup through t, and
// must return a driver connected to an available backend. These tests deliberately
// use only public protocol operations, never backend-specific state inspection.
func Run(t *testing.T, factory func(*testing.T) protocol.Driver) {
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
			cfg := protocol.ResourceConfig{
				Admission: protocol.Config{Capacity: 3, MaxQueued: 64, MaxQueuedPerTenant: 64},
				LeaseTTL:  30 * time.Second, MaxWait: 30 * time.Second,
				SubmitWindow: 30 * time.Second, Retention: 100 * time.Millisecond, MaxRecords: 256,
			}
			identity := protocol.NewRequest(time.Now().Add(time.Minute))
			test.run(&harness{t: t, driver: factory(t), resource: "conformance-" + identity.ID, cfg: cfg})
		})
	}
}

// harness binds one backend, isolated resource and protocol configuration.
type harness struct {
	// t owns assertions and backend cleanup for this isolated subtest.
	t *testing.T
	// driver is the backend under conformance testing.
	driver protocol.Driver
	// resource uniquely isolates this subtest from other backend users.
	resource string
	// cfg supplies the immutable policy used for requests in this subtest.
	cfg protocol.ResourceConfig
}

// request creates a fresh identity with deadlines inside the configured backend windows.
func (h *harness) request(tenant string, weight int) protocol.Request {
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := protocol.NewRequest(now.Add(h.cfg.SubmitWindow - time.Millisecond))
	r.Resource, r.Tenant, r.Weight, r.Config = h.resource, tenant, weight, h.cfg
	r.WaitUntil = now.Add(h.cfg.MaxWait - time.Millisecond)
	return r
}

// execute invokes one bounded backend operation, allowing a larger bound in stress mode.
func (h *harness) execute(kind protocol.Operation, request protocol.Request, lease string) (protocol.Result, error) {
	timeout := 5 * time.Second
	if os.Getenv("AIRLOCK_STRESS") == "1" {
		// Race instrumentation and CPU=1 amplify snapshot/CAS contention.
		// Keep the stress run bounded without turning this into a latency SLA.
		timeout = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return Invoke(ctx, h.driver, protocol.Command{Kind: kind, Request: request, LeaseID: lease})
}

// must requires the expected successful status and validates grant accounting.
func (h *harness) must(kind protocol.Operation, request protocol.Request, lease string, status protocol.Status) protocol.Result {
	h.t.Helper()
	result, err := h.execute(kind, request, lease)
	if err != nil || result.Status != status {
		h.t.Fatalf("%s tenant=%q: result=%+v err=%v; want %s", kind, request.Tenant, result, err, status)
	}
	if result.Weight != request.Weight || result.Revision == 0 {
		h.t.Fatalf("%s invalid result accounting: %+v", kind, result)
	}
	if status == protocol.Granted && (result.LeaseID == "" || result.Remaining <= 0 || result.Remaining > request.Config.LeaseTTL) {
		h.t.Fatalf("invalid grant: %+v", result)
	}
	return result
}

// wantError requires a specific error identity while preserving the returned protocol result.
func (h *harness) wantError(kind protocol.Operation, request protocol.Request, lease string, want error) protocol.Result {
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
	first := h.must(protocol.Submit, r, "", protocol.Granted)
	retry := h.must(protocol.Submit, r, "", protocol.Granted)
	if first.LeaseID != retry.LeaseID || retry.Remaining > first.Remaining || retry.Revision <= first.Revision {
		h.t.Fatalf("retry changed grant identity/TTL/revision: first=%+v retry=%+v", first, retry)
	}
	for _, change := range []func(*protocol.Request){
		func(r *protocol.Request) { r.Owner = protocol.NewRequest(time.Now()).Owner },
		func(r *protocol.Request) { r.Weight = 1 },
		func(r *protocol.Request) { r.Tenant = "other" },
		func(r *protocol.Request) { r.WaitUntil = r.WaitUntil.Add(-time.Millisecond) },
	} {
		conflict := r
		change(&conflict)
		h.wantError(protocol.Submit, conflict, "", protocol.ErrRequestConflict)
	}
	conflict := r
	conflict.Config.Admission.Capacity++
	h.wantError(protocol.Submit, conflict, "", protocol.ErrConfigConflict)
	h.wantError(protocol.Renew, r, "incorrect-lease", protocol.ErrLeaseLost)
	h.wantError(protocol.Release, r, "incorrect-lease", protocol.ErrLeaseLost)
	h.must(protocol.Observe, r, "", protocol.Granted)
	other := h.request("b", 2)
	h.must(protocol.Submit, other, "", protocol.Queued)
	h.must(protocol.Release, r, first.LeaseID, protocol.Released)
	h.must(protocol.Release, r, first.LeaseID, protocol.Released)
	h.must(protocol.Submit, r, "", protocol.Released)
	h.wantError(protocol.Renew, r, first.LeaseID, protocol.ErrLeaseLost)
	granted := h.must(protocol.Observe, other, "", protocol.Granted)
	third := h.request("c", 2)
	h.must(protocol.Submit, third, "", protocol.Queued)
	h.must(protocol.Release, other, granted.LeaseID, protocol.Released)
	h.must(protocol.Observe, third, "", protocol.Granted)
}

// cancelBeforeSubmit verifies tombstones prevent late submissions from acquiring capacity.
func cancelBeforeSubmit(h *harness) {
	r := h.request("a", 3)
	h.must(protocol.Cancel, r, "", protocol.Cancelled)
	h.must(protocol.Submit, r, "", protocol.Cancelled)
	h.must(protocol.Cancel, r, "", protocol.Cancelled)
	h.must(protocol.Observe, r, "", protocol.Cancelled)
	fresh := h.request("b", 3)
	h.must(protocol.Submit, fresh, "", protocol.Granted)
	h.must(protocol.Cancel, fresh, "", protocol.Cancelled)
	h.must(protocol.Submit, fresh, "", protocol.Cancelled)
	h.must(protocol.Submit, h.request("c", 3), "", protocol.Granted)
}

// weightValidation checks invalid weights leave full capacity available.
func weightValidation(h *harness) {
	for _, weight := range []int{-1, 0, 4} {
		want := protocol.ErrInvalidWeight
		if weight > h.cfg.Admission.Capacity {
			want = protocol.ErrWeightExceedsCapacity
		}
		h.wantError(protocol.Submit, h.request("a", weight), "", want)
	}
	h.must(protocol.Submit, h.request("b", h.cfg.Admission.Capacity), "", protocol.Granted)
}

// protectedHead verifies small requests cannot bypass a selected whole-weight request.
func protectedHead(h *harness) {
	holder := h.request("holder", 2)
	grant := h.must(protocol.Submit, holder, "", protocol.Granted)
	large, small := h.request("large", 3), h.request("small", 1)
	h.must(protocol.Submit, large, "", protocol.Queued)
	h.must(protocol.Submit, small, "", protocol.Queued)
	for i := 0; i < 3; i++ {
		h.must(protocol.Observe, small, "", protocol.Queued)
	}
	h.must(protocol.Release, holder, grant.LeaseID, protocol.Released)
	h.must(protocol.Observe, small, "", protocol.Queued)
	grant = h.must(protocol.Observe, large, "", protocol.Granted)
	h.must(protocol.Release, large, grant.LeaseID, protocol.Released)
	h.must(protocol.Observe, small, "", protocol.Granted)
}

// queueBounds checks global/tenant limits and retained rejection outcomes.
func queueBounds(h *harness) {
	h.cfg.Admission.MaxQueued = 2
	h.cfg.Admission.MaxQueuedPerTenant = 1
	holder := h.request("holder", 3)
	h.must(protocol.Submit, holder, "", protocol.Granted)
	a := h.request("a", 1)
	h.must(protocol.Submit, a, "", protocol.Queued)
	h.wantError(protocol.Submit, h.request("a", 1), "", protocol.ErrTenantQueueFull)
	h.must(protocol.Submit, h.request("b", 1), "", protocol.Queued)
	rejected := h.request("c", 1)
	if got := h.wantError(protocol.Submit, rejected, "", protocol.ErrQueueFull); got.Status != protocol.Rejected {
		h.t.Fatalf("queue rejection status=%s", got.Status)
	}
	h.must(protocol.Cancel, a, "", protocol.Cancelled)
	h.wantError(protocol.Submit, rejected, "", protocol.ErrQueueFull)
	h.must(protocol.Submit, h.request("c", 1), "", protocol.Queued)
}

// groupBounds verifies reserved group waiting budgets and per-tenant bounds.
func groupBounds(h *harness) {
	h.cfg.Admission.MaxQueued = 4
	h.cfg.Admission.MaxQueuedPerTenant = 1
	h.cfg.Admission.Priority = &protocol.PriorityPolicy{
		Groups:       []protocol.PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 2}, {Name: "low", Turns: 1, MaxQueued: 2}},
		DefaultGroup: "low", TenantGroups: map[string]string{"h1": "high", "h2": "high", "h3": "high"},
	}
	h.must(protocol.Submit, h.request("holder", 3), "", protocol.Granted)
	a := h.request("h1", 1)
	h.must(protocol.Submit, a, "", protocol.Queued)
	h.wantError(protocol.Submit, h.request("h1", 1), "", protocol.ErrTenantQueueFull)
	h.must(protocol.Submit, h.request("h2", 1), "", protocol.Queued)
	h.wantError(protocol.Submit, h.request("h3", 1), "", protocol.ErrGroupQueueFull)
	h.must(protocol.Submit, h.request("l1", 1), "", protocol.Queued)
	h.must(protocol.Cancel, a, "", protocol.Cancelled)
	h.must(protocol.Submit, h.request("h3", 1), "", protocol.Queued)
}

// priorityRotation verifies weighted group turns are independent of observation frequency.
func priorityRotation(h *harness) {
	h.cfg.Admission.Capacity = 1
	h.cfg.Admission.Priority = &protocol.PriorityPolicy{
		Groups:       []protocol.PriorityGroup{{Name: "high", Turns: 8, MaxQueued: 24}, {Name: "normal", Turns: 3, MaxQueued: 12}, {Name: "low", Turns: 1, MaxQueued: 8}},
		DefaultGroup: "low", TenantGroups: map[string]string{"h1": "high", "h2": "high", "m": "normal"},
	}
	holder := h.request("holder", 1)
	grant := h.must(protocol.Submit, holder, "", protocol.Granted)
	queues := map[string][]protocol.Request{}
	for _, spec := range []struct {
		tenant string
		count  int
	}{{"l", 2}, {"m", 6}, {"h1", 8}, {"h2", 8}} {
		for i := 0; i < spec.count; i++ {
			r := h.request(spec.tenant, 1)
			queues[spec.tenant] = append(queues[spec.tenant], r)
			h.must(protocol.Submit, r, "", protocol.Queued)
		}
	}
	h.must(protocol.Release, holder, grant.LeaseID, protocol.Released)
	for cycle := 0; cycle < 2; cycle++ {
		for _, tenant := range []string{"h1", "h2", "h1", "h2", "h1", "h2", "h1", "h2", "m", "m", "m", "l"} {
			r := queues[tenant][0]
			// Poll all other tenants first. Observation frequency cannot change turns.
			for _, other := range []string{"l", "m", "h2", "h1"} {
				if other != tenant && len(queues[other]) > 0 {
					h.must(protocol.Observe, queues[other][0], "", protocol.Queued)
				}
			}
			g := h.must(protocol.Observe, r, "", protocol.Granted)
			h.must(protocol.Release, r, g.LeaseID, protocol.Released)
			queues[tenant] = queues[tenant][1:]
		}
	}
}

// tenantRotation checks FIFO round-robin order after a tenant leaves and rejoins.
func tenantRotation(h *harness) {
	h.cfg.Admission.Capacity = 1
	holder := h.request("holder", 1)
	g := h.must(protocol.Submit, holder, "", protocol.Granted)
	a1, a2, b, c := h.request("a", 1), h.request("a", 1), h.request("b", 1), h.request("c", 1)
	for _, r := range []protocol.Request{a1, a2, b, c} {
		h.must(protocol.Submit, r, "", protocol.Queued)
	}
	h.must(protocol.Cancel, b, "", protocol.Cancelled)
	b2 := h.request("b", 1)
	h.must(protocol.Submit, b2, "", protocol.Queued)
	h.must(protocol.Release, holder, g.LeaseID, protocol.Released)
	for _, r := range []protocol.Request{a1, c, b2, a2} {
		g = h.must(protocol.Observe, r, "", protocol.Granted)
		h.must(protocol.Release, r, g.LeaseID, protocol.Released)
	}
}

// selectedCancel verifies cancellation does not refund a consumed group selection turn.
func selectedCancel(h *harness) {
	h.cfg.Admission.Priority = &protocol.PriorityPolicy{
		Groups:       []protocol.PriorityGroup{{Name: "high", Turns: 2, MaxQueued: 8}, {Name: "low", Turns: 1, MaxQueued: 8}},
		DefaultGroup: "low", TenantGroups: map[string]string{"h": "high"},
	}
	holder := h.request("holder", 2)
	g := h.must(protocol.Submit, holder, "", protocol.Granted)
	h1, h2, h3, low := h.request("h", 3), h.request("h", 3), h.request("h", 3), h.request("low", 3)
	for _, r := range []protocol.Request{h1, h2, h3, low} {
		h.must(protocol.Submit, r, "", protocol.Queued)
	}
	h.must(protocol.Cancel, h1, "", protocol.Cancelled)
	h.must(protocol.Release, holder, g.LeaseID, protocol.Released)
	for _, r := range []protocol.Request{h2, low, h3} {
		g = h.must(protocol.Observe, r, "", protocol.Granted)
		h.must(protocol.Release, r, g.LeaseID, protocol.Released)
	}
}

// queueExpiry checks deadline expiry frees waiting space without reviving the request.
func queueExpiry(h *harness) {
	h.cfg.MaxWait = 300 * time.Millisecond
	h.cfg.Admission.MaxQueued = 1
	h.cfg.Admission.MaxQueuedPerTenant = 1
	holder := h.request("holder", 3)
	h.must(protocol.Submit, holder, "", protocol.Granted)
	queued := h.request("a", 3)
	h.must(protocol.Submit, queued, "", protocol.Queued)
	sleepUntil(queued.WaitUntil.Add(30 * time.Millisecond))
	h.must(protocol.Observe, queued, "", protocol.Expired)
	h.must(protocol.Submit, queued, "", protocol.Expired)
	h.must(protocol.Submit, h.request("b", 3), "", protocol.Queued)
}

// leaseExpiry verifies renewal identity, capacity reclamation and terminal release behavior.
func leaseExpiry(h *harness) {
	h.cfg.LeaseTTL = 300 * time.Millisecond
	r := h.request("a", 3)
	g := h.must(protocol.Submit, r, "", protocol.Granted)
	renewed := h.must(protocol.Renew, r, g.LeaseID, protocol.Granted)
	start := time.Now()
	if renewed.LeaseID != g.LeaseID {
		h.t.Fatal("renew changed lease ID")
	}
	waiting := h.request("b", 3)
	h.must(protocol.Submit, waiting, "", protocol.Queued)
	sleepUntil(start.Add(h.cfg.LeaseTTL + 100*time.Millisecond))
	h.must(protocol.Observe, waiting, "", protocol.Granted)
	h.must(protocol.Observe, r, "", protocol.Expired)
	h.wantError(protocol.Renew, r, g.LeaseID, protocol.ErrLeaseLost)
	h.must(protocol.Release, r, g.LeaseID, protocol.Expired)
	h.must(protocol.Observe, waiting, "", protocol.Granted)
}

// historyGC verifies live records survive GC and terminal retention prevents premature ID reuse.
func historyGC(h *harness) {
	h.cfg.MaxRecords = 1
	h.cfg.SubmitWindow = 300 * time.Millisecond
	r := h.request("a", 3)
	g := h.must(protocol.Submit, r, "", protocol.Granted)
	sleepUntil(r.SubmitUntil.Add(30 * time.Millisecond))
	// Submission cutoff and the recovery window never collect a live lease.
	h.must(protocol.Renew, r, g.LeaseID, protocol.Granted)
	h.must(protocol.Submit, r, "", protocol.Granted)
	h.wantError(protocol.Submit, h.request("b", 1), "", protocol.ErrHistoryFull)
	h.must(protocol.Release, r, g.LeaseID, protocol.Released)
	h.wantError(protocol.Submit, h.request("b", 1), "", protocol.ErrHistoryFull)
	time.Sleep(h.cfg.Retention + 30*time.Millisecond)
	h.must(protocol.Submit, h.request("b", 3), "", protocol.Granted)
	h.wantError(protocol.Submit, r, "", protocol.ErrSubmitExpired)
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
	requests := make([]protocol.Request, clients)
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
		go func(r protocol.Request) {
			defer workers.Done()
			<-start
			result, err := h.execute(protocol.Submit, r, "")
			if err == nil && result.Status == protocol.Granted {
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
			for result.Status == protocol.Queued && time.Now().Before(deadline) {
				// Observe may atomically rewrite a bounded resource snapshot.
				// Avoid a 24-client polling storm under race/coverage instruments;
				// this tests capacity and completion, not an operation-latency SLA.
				time.Sleep(50 * time.Millisecond)
				result, err = h.execute(protocol.Observe, r, "")
				if err != nil {
					h.t.Errorf("concurrent observe: %v", err)
					return
				}
				if result.Status == protocol.Granted {
					if used := inUse.Add(int64(r.Weight)); used > int64(h.cfg.Admission.Capacity) {
						h.t.Errorf("concurrent grants own %d > %d", used, h.cfg.Admission.Capacity)
					}
				}
			}
			if result.Status != protocol.Granted || result.Weight != r.Weight || result.LeaseID == "" {
				h.t.Errorf("concurrent grant invalid: %+v", result)
				return
			}
			// Stop simulated work before release can grant the next request.
			inUse.Add(-int64(r.Weight))
			for i := 0; i < 2; i++ {
				released, releaseErr := h.execute(protocol.Release, r, result.LeaseID)
				if releaseErr != nil || released.Status != protocol.Released {
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

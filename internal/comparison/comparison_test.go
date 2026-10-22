// Package comparison contains experiments, not a public Airlock implementation.
package comparison

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)

// model is a deterministic policy simulator. It does not reproduce goroutine
// scheduling or semaphore's internal waiter queue. Every job costs one token;
// tests explicitly decide when jobs finish. Weighted enforces actual capacity.
type model struct {
	policy                   string
	global                   *semaphore.Weighted
	local                    map[string]*semaphore.Weighted
	localLimit               int64
	queue                    []job
	order                    []string // active waiting tenants, in rotation order
	used                     int
	maxQueue, maxTenantQueue int
}

type job struct {
	id     int
	tenant string
}

func newModel(policy string, capacity, perTenant int) *model {
	return &model{policy: policy, global: semaphore.NewWeighted(int64(capacity)),
		local: make(map[string]*semaphore.Weighted), localLimit: int64(perTenant),
		maxQueue: 10000, maxTenantQueue: 10000}
}

func (m *model) enqueue(j job) bool {
	n := 0
	for _, q := range m.queue {
		if q.tenant == j.tenant {
			n++
		}
	}
	if len(m.queue) >= m.maxQueue || n >= m.maxTenantQueue {
		return false
	}
	if n == 0 {
		m.order = append(m.order, j.tenant)
	}
	m.queue = append(m.queue, j)
	return true
}

func (m *model) remove(i int) job {
	j := m.queue[i]
	m.queue = append(m.queue[:i], m.queue[i+1:]...)
	remaining := false
	for _, q := range m.queue {
		if q.tenant == j.tenant {
			remaining = true
			break
		}
	}
	if !remaining {
		for k, tenant := range m.order {
			if tenant == j.tenant {
				m.order = append(m.order[:k], m.order[k+1:]...)
				break
			}
		}
	}
	return j
}

func (m *model) cancel(id int) bool {
	for i, j := range m.queue {
		if j.id == id {
			m.remove(i)
			return true
		}
	}
	return false
}

// next models FIFO, a favorable two-level baseline that skips locally blocked
// tenants without holding global capacity, or tenant round-robin. This favorable
// baseline is stronger than naive nested blocking Acquire calls.
func (m *model) next() (job, bool) {
	if len(m.queue) == 0 {
		return job{}, false
	}
	i := 0
	if m.policy == "round-robin" {
		for k, j := range m.queue {
			if j.tenant == m.order[0] {
				i = k
				break
			}
		}
	}
	if m.policy == "two-level" {
		i = -1
		for k, j := range m.queue {
			s := m.local[j.tenant]
			if s == nil {
				s = semaphore.NewWeighted(m.localLimit)
				m.local[j.tenant] = s
			}
			if s.TryAcquire(1) {
				i = k
				break
			}
		}
		if i == -1 {
			return job{}, false
		}
	}
	if !m.global.TryAcquire(1) {
		if m.policy == "two-level" {
			m.local[m.queue[i].tenant].Release(1)
		}
		return job{}, false
	}
	j := m.remove(i)
	if m.policy == "round-robin" && len(m.order) > 0 && m.order[0] == j.tenant {
		m.order = append(m.order[1:], j.tenant)
	}
	m.used++
	return j, true
}

func (m *model) release(j job) {
	m.global.Release(1)
	if m.policy == "two-level" {
		m.local[j.tenant].Release(1)
	}
	m.used--
}

func TestFloodAdmissionOrder(t *testing.T) {
	for _, policy := range []string{"fifo", "two-level", "round-robin"} {
		t.Run(policy, func(t *testing.T) {
			m := newModel(policy, 1, 1)
			// All arrivals are waiting behind an existing holder. With one slot,
			// release-to-next-admission counts are independent of wall-clock noise.
			for i := 0; i < 32; i++ {
				m.enqueue(job{i, "A"})
			}
			m.enqueue(job{32, "B"})
			rank := 0
			for {
				j, ok := m.next()
				if !ok {
					break
				}
				rank++
				m.release(j)
				if j.tenant == "B" {
					break
				}
			}
			want := 33
			if policy == "round-robin" {
				want = 2
			}
			if rank != want {
				t.Fatalf("B admission rank=%d, want %d", rank, want)
			}
			t.Logf("B admission rank: %d (32 A requests ahead)", rank)
		})
	}
}

func TestSingleTenantUtilization(t *testing.T) {
	for _, policy := range []string{"fifo", "two-level", "round-robin"} {
		t.Run(policy, func(t *testing.T) {
			m := newModel(policy, 8, 2)
			for i := 0; i < 16; i++ {
				m.enqueue(job{i, "A"})
			}
			var held []job
			for {
				j, ok := m.next()
				if !ok {
					break
				}
				held = append(held, j)
			}
			want := 8
			if policy == "two-level" {
				want = 2
			}
			if len(held) != want {
				t.Fatalf("occupied=%d, want %d", len(held), want)
			}
			t.Logf("occupied capacity: %d/8; fixed tenant cap=2 for two-level", len(held))
			for _, j := range held {
				m.release(j)
			}
		})
	}
}

func TestTwoLevelCapProtectsOtherTenant(t *testing.T) {
	m := newModel("two-level", 4, 2)
	for i := 0; i < 8; i++ {
		m.enqueue(job{i, "A"})
	}
	m.enqueue(job{8, "B"})
	var held []job
	for {
		j, ok := m.next()
		if !ok {
			break
		}
		held = append(held, j)
	}
	if len(held) != 3 || held[2].tenant != "B" {
		t.Fatalf("unexpected admissions: %v", held)
	}
	for _, j := range held {
		m.release(j)
	}
}

func TestRoundRobinOrderAndCancellation(t *testing.T) {
	m := newModel("round-robin", 1, 1)
	for i, tenant := range []string{"A", "A", "B", "B", "C", "C"} {
		m.enqueue(job{i, tenant})
	}
	if !m.cancel(4) || !m.cancel(5) || m.cancel(5) {
		t.Fatal("cancellation bookkeeping")
	}
	var got string
	for {
		j, ok := m.next()
		if !ok {
			break
		}
		got += j.tenant
		m.release(j)
	}
	if got != "ABAB" || len(m.order) != 0 || m.used != 0 {
		t.Fatalf("order=%s, state=%+v", got, m)
	}
}

func TestQueueBounds(t *testing.T) {
	m := newModel("round-robin", 1, 1)
	m.maxQueue, m.maxTenantQueue = 3, 2
	for _, j := range []job{{0, "A"}, {1, "A"}} {
		if !m.enqueue(j) {
			t.Fatal("unexpected rejection")
		}
	}
	if m.enqueue(job{2, "A"}) {
		t.Fatal("tenant queue limit exceeded")
	}
	if !m.enqueue(job{3, "B"}) {
		t.Fatal("A prevented B from queueing")
	}
	if m.enqueue(job{4, "C"}) {
		t.Fatal("global queue limit exceeded")
	}
	m.cancel(0)
	if !m.enqueue(job{5, "C"}) {
		t.Fatal("cancel did not restore queue capacity")
	}
}

// acquireNested is the actual tenant-first composition, independent of models.
// It must roll back the tenant token if acquiring the global token fails.
func acquireNested(ctx context.Context, global, tenant *semaphore.Weighted) (func(), error) {
	if err := tenant.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	if err := global.Acquire(ctx, 1); err != nil {
		tenant.Release(1)
		return nil, err
	}
	return func() { global.Release(1); tenant.Release(1) }, nil
}

func TestWeightedCancellationReclaimsCapacity(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprintf("nested=%v", nested), func(t *testing.T) {
			global, tenant := semaphore.NewWeighted(1), semaphore.NewWeighted(1)
			if !global.TryAcquire(1) {
				t.Fatal("initial capacity")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			var err error
			if nested {
				_, err = acquireNested(ctx, global, tenant)
			} else {
				err = global.Acquire(ctx, 1)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v", err)
			}
			global.Release(1)
			if !global.TryAcquire(1) || !tenant.TryAcquire(1) {
				t.Fatal("capacity leaked")
			}
			global.Release(1)
			tenant.Release(1)
		})
	}
}

func TestNestedCancellationReleaseRace(t *testing.T) {
	for i := 0; i < 200; i++ {
		global, tenant := semaphore.NewWeighted(1), semaphore.NewWeighted(1)
		global.TryAcquire(1)
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancel() }()
		go func() { defer wg.Done(); global.Release(1) }()
		release, err := acquireNested(ctx, global, tenant)
		if err == nil {
			release()
		} else if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		wg.Wait()
		if !global.TryAcquire(1) || !tenant.TryAcquire(1) {
			t.Fatal("capacity leaked")
		}
		global.Release(1)
		tenant.Release(1)
	}
}

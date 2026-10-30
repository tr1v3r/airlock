package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestIntegrationPauseMigratesLegacyConstraint(t *testing.T) {
	db := integrationDB(t)
	ctx := context.Background()
	name := strings.Repeat("Pause_", 7) + strings.ReplaceAll(airlock.NewRequest(time.Now()).ID, "-", "")[:12]
	d, err := New(db, uniqueNamespace(), WithTable(name))
	if err != nil {
		t.Fatal(err)
	}
	// Build the exact pre-pause schema, including PostgreSQL-generated check names.
	ddl := d.schema()
	ddl[0] = strings.ReplaceAll(ddl[0], "capacity>=0", "capacity>0")
	for _, statement := range ddl[:2] {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", d.requestTable))
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", d.resourceTable))
	})
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT operator_capacity_ceiling CHECK (capacity<=100)`, d.resourceTable)); err != nil {
		t.Fatal(err)
	}
	r := request(time.Now())
	for i := 0; i < 2; i++ {
		if err := d.EnsureSchema(ctx); err != nil {
			t.Fatal(err)
		}
	}
	grant, err := d.Submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Resize(ctx, airlock.ResizeRequest{Resource: r.Resource, Capacity: 0, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(db, d.namespace, WithTable(name))
	if err != nil {
		t.Fatal(err)
	}
	if s, e := restarted.InspectResource(ctx, r.Resource); e != nil || s.Capacity != 0 || s.Used != r.Weight {
		t.Fatal(s, e)
	}
	if _, err := restarted.Release(ctx, airlock.LeaseRef{Request: r, ID: grant.LeaseID}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET capacity=-1", d.resourceTable)); err == nil {
		t.Fatal("negative persisted capacity accepted")
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET capacity=101", d.resourceTable)); err == nil {
		t.Fatal("operator constraint was removed")
	}
	if _, err := restarted.Resize(ctx, airlock.ResizeRequest{Resource: r.Resource, Capacity: 2, ExpectedVersion: 2}); err != nil {
		t.Fatal(err)
	}
}

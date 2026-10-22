package airlock

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestDistributedRejectsNonPortableTextBeforeBackend(t *testing.T) {
	for _, tc := range []struct{ name, value string }{{"invalid UTF-8", string([]byte{0xff})}, {"embedded NUL", "a\x00b"}, {"oversized", strings.Repeat("x", 257)}} {
		t.Run(tc.name, func(t *testing.T) {
			d := &clientStateDriver{}
			c := clientNew(t, d, clientOptions())
			if lease, err := c.AcquireNFor(context.Background(), tc.value, 1); lease != nil || !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("invalid tenant admitted: lease=%v err=%v", lease, err)
			}
			if len(d.commands()) != 0 {
				t.Fatal("invalid tenant contacted backend")
			}
			for _, field := range []string{"resource", "group", "mapping key", "mapping value"} {
				t.Run(field, func(t *testing.T) {
					o := clientOptions()
					o.Config.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "default", Turns: 1, MaxQueued: 16}}, DefaultGroup: "default", TenantGroups: map[string]string{"tenant": "default"}}
					switch field {
					case "resource":
						o.Resource = tc.value
					case "group":
						o.Config.Admission.Priority.Groups[0].Name = tc.value
						o.Config.Admission.Priority.DefaultGroup = tc.value
						o.Config.Admission.Priority.TenantGroups["tenant"] = tc.value
					case "mapping key":
						o.Config.Admission.Priority.TenantGroups = map[string]string{tc.value: "default"}
					case "mapping value":
						o.Config.Admission.Priority.TenantGroups["tenant"] = tc.value
					}
					if _, err := NewDistributed(d, o); !errors.Is(err, ErrInvalidConfig) {
						t.Fatalf("invalid %s accepted: %v", field, err)
					}
				})
			}
		})
	}
}

func TestDistributedUnicodeNamesSurviveSnapshotRoundTrip(t *testing.T) {
	d := &clientStateDriver{}
	o := clientOptions()
	o.Resource = "资源🔑"
	o.Config.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "优先", Turns: 1, MaxQueued: 16}}, DefaultGroup: "优先", TenantGroups: map[string]string{"租户": "优先"}}
	c := clientNew(t, d, o)
	lease, err := c.AcquireFor(context.Background(), "租户")
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	clientClosed(t, lease)
}

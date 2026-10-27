package airlock

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func stateTestConfig() ResourceConfig {
	return ResourceConfig{Admission: Config{Capacity: 3, MaxQueued: 100, MaxQueuedPerTenant: 100}, LeaseTTL: time.Second, MaxWait: time.Minute, SubmitWindow: time.Minute, Retention: time.Second, MaxRecords: 1000}
}

func stateTestRequest(now time.Time, cfg ResourceConfig, tenant string, weight int) Request {
	r := NewRequest(now.Add(10 * time.Second))
	r.Resource, r.Config, r.Tenant, r.Weight = "test-resource", cfg, tenant, weight
	r.WaitUntil = now.Add(30 * time.Second)
	return r
}

func TestNewRequestIdentity(t *testing.T) {
	until := time.Date(2026, 10, 23, 12, 0, 0, 123456789, time.FixedZone("offset", 3600))
	a, b := NewRequest(until), NewRequest(until)
	if a.ID == b.ID || a.Owner == b.Owner || a.ID == "" || !validSecret(a.Owner) {
		t.Fatalf("identities not independent: %+v %+v", a, b)
	}
	if a.SubmitUntil.Location() != time.UTC || !a.SubmitUntil.Equal(until.Truncate(time.Millisecond)) {
		t.Fatalf("deadline not normalized: %v", a.SubmitUntil)
	}

	for _, secret := range []string{"", "0", strings.Repeat("g", 32), strings.Repeat("A", 32), strings.Repeat("0", 30), strings.Repeat("0", 34)} {
		if validSecret(secret) {
			t.Errorf("invalid secret accepted: %q", secret)
		}
	}
}

func TestDriverConfigValidation(t *testing.T) {
	cases := map[string]func(*ResourceConfig){
		"capacity negative": func(c *ResourceConfig) { c.Admission.Capacity = -1 },
		"capacity zero":     func(c *ResourceConfig) { c.Admission.Capacity = 0 },
		"queue upper":       func(c *ResourceConfig) { c.Admission.MaxQueued = 10001 },
		"records zero":      func(c *ResourceConfig) { c.MaxRecords = 0 },
		"records upper":     func(c *ResourceConfig) { c.MaxRecords = 10001 },
		"lease zero":        func(c *ResourceConfig) { c.LeaseTTL = 0 },
		"wait negative":     func(c *ResourceConfig) { c.MaxWait = -time.Second },
		"fractional":        func(c *ResourceConfig) { c.Retention = time.Second + time.Nanosecond },
		"lease upper":       func(c *ResourceConfig) { c.LeaseTTL = time.Hour + time.Millisecond },
		"wait upper":        func(c *ResourceConfig) { c.MaxWait = time.Hour + time.Millisecond },
		"submit upper":      func(c *ResourceConfig) { c.SubmitWindow = time.Hour + time.Millisecond },
		"retention upper":   func(c *ResourceConfig) { c.Retention = 24*time.Hour + time.Millisecond },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := stateTestConfig()
			mutate(&c)
			if !errors.Is(validConfig(c), ErrInvalidConfig) {
				t.Fatal("invalid config accepted")
			}
		})
	}

	c := stateTestConfig()
	c.LeaseTTL = time.Hour
	c.MaxWait = time.Hour
	c.SubmitWindow = time.Hour
	c.Retention = 24 * time.Hour
	c.MaxRecords = 10000
	c.Admission.Capacity = maxInt
	c.Admission.MaxQueued = 10000
	if err := validConfig(c); err != nil {
		t.Fatal(err)
	}
}

func TestDriverRequestValidation(t *testing.T) {
	now := time.Unix(1000, 0)
	cases := []struct {
		name   string
		change func(*Request)
		want   error
	}{
		{"config", func(r *Request) { r.Config.MaxRecords = 0 }, ErrInvalidConfig},
		{"resource", func(r *Request) { r.Resource = "" }, ErrInvalidRequest},
		{"long resource", func(r *Request) { r.Resource = strings.Repeat("x", 257) }, ErrInvalidRequest},
		{"long tenant", func(r *Request) { r.Tenant = strings.Repeat("x", 257) }, ErrInvalidRequest},
		{"owner", func(r *Request) { r.Owner = "wrong" }, ErrInvalidRequest},
		{"id deadline", func(r *Request) { r.SubmitUntil = r.SubmitUntil.Add(time.Second) }, ErrInvalidRequest},
		{"id suffix", func(r *Request) { r.ID += "0" }, ErrInvalidRequest},
		{"negative epoch", func(r *Request) {
			x := NewRequest(time.Unix(-1, 0))
			r.ID = x.ID
			r.SubmitUntil = x.SubmitUntil
		}, ErrInvalidRequest},
		{"fractional submit", func(r *Request) { r.SubmitUntil = r.SubmitUntil.Add(time.Nanosecond) }, ErrInvalidRequest},
		{"fractional wait", func(r *Request) { r.WaitUntil = r.WaitUntil.Add(time.Nanosecond) }, ErrInvalidRequest},
		{"wait bound", func(r *Request) { r.WaitUntil = r.SubmitUntil.Add(r.Config.MaxWait + time.Millisecond) }, ErrInvalidRequest},
		{"zero weight", func(r *Request) { r.Weight = 0 }, ErrInvalidWeight},
		{"negative weight", func(r *Request) { r.Weight = -1 }, ErrInvalidWeight},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := stateTestRequest(now, stateTestConfig(), "", 1)
			tc.change(&r)
			if err := validRequest(r); !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
		})
	}

	r := stateTestRequest(now, stateTestConfig(), strings.Repeat("x", 256), 1)
	r.Resource = strings.Repeat("r", 256)
	r.WaitUntil = r.SubmitUntil.Add(r.Config.MaxWait)
	if err := validRequest(r); err != nil {
		t.Fatal(err)
	}
}

func TestDriverPriorityConfigBounds(t *testing.T) {
	for _, name := range []string{"mapping count", "tenant length", "mapped group length", "group name length"} {
		t.Run(name, func(t *testing.T) {
			c := stateTestConfig()
			p := &PriorityPolicy{Groups: []PriorityGroup{{Name: "group", Turns: 1, MaxQueued: 100}}, DefaultGroup: "group", TenantGroups: map[string]string{}}
			c.Admission.Priority = p

			switch name {
			case "mapping count":
				for i := 0; i < 10001; i++ {
					p.TenantGroups[fmt.Sprint(i)] = "group"
				}
			case "tenant length":
				p.TenantGroups[strings.Repeat("x", 257)] = "group"
			case "mapped group length":
				p.TenantGroups["tenant"] = strings.Repeat("g", 257)
			case "group name length":
				p.Groups[0].Name = strings.Repeat("g", 257)
				p.DefaultGroup = p.Groups[0].Name
			}

			if err := validConfig(c); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestDriverTextEncoding(t *testing.T) {
	for _, bad := range []string{string([]byte{0xff}), "has\x00nul"} {
		for _, field := range []string{"resource", "tenant"} {
			r := stateTestRequest(time.Unix(1000, 0), stateTestConfig(), "", 1)
			if field == "resource" {
				r.Resource = bad
			} else {
				r.Tenant = bad
			}

			if err := validRequest(r); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("%s accepted: %v", field, err)
			}
		}

		for _, field := range []string{"group", "tenant", "mapping"} {
			c := stateTestConfig()
			c.Admission.Priority = &PriorityPolicy{Groups: []PriorityGroup{{Name: "g", Turns: 1, MaxQueued: 100}}, DefaultGroup: "g", TenantGroups: map[string]string{"a": "g"}}

			switch field {
			case "group":
				c.Admission.Priority.Groups[0].Name = bad
			case "tenant":
				c.Admission.Priority.TenantGroups[bad] = "g"
			case "mapping":
				c.Admission.Priority.TenantGroups["a"] = bad
			}

			if err := validConfig(c); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("%s accepted: %v", field, err)
			}
		}
	}
}

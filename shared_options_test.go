package airlock

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSharedDefaultsInteroperateWithExplicitConfig(t *testing.T) {
	d := &clientStateDriver{}
	options := SharedOptions{Resource: "jobs", Config: ResourceConfig{Admission: Config{Capacity: 2}}}
	implicit := clientNew(t, d, options)
	// Defaults must not mutate the caller's value or perform backend I/O.
	if options.Config.LeaseTTL != 0 || options.OperationTimeout != 0 || len(d.commands()) != 0 {
		t.Fatal("construction changed input or contacted backend")
	}
	explicitOptions := SharedOptions{
		Resource: "jobs",
		Config: ResourceConfig{
			Admission: Config{Capacity: 2}, LeaseTTL: 30 * time.Second,
			MaxWait: 30 * time.Second, SubmitWindow: 5 * time.Second,
			Retention: time.Minute, MaxRecords: 10000,
		},
		OperationTimeout: 5 * time.Second, PollInterval: 50 * time.Millisecond,
	}
	if !reflect.DeepEqual(implicit.options, explicitOptions) {
		t.Fatalf("resolved options: %+v", implicit.options)
	}
	explicit := clientNew(t, d, explicitOptions)
	first := clientAcquire(t, implicit)
	second := clientAcquire(t, explicit)
	if first.Weight() != 1 || second.Weight() != 1 {
		t.Fatal("unexpected weight")
	}
	// Zero queue limits stay disabled, even though MaxWait has a default.
	if _, err := implicit.Acquire(context.Background()); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected immediate queue rejection, got %v", err)
	}
}

func TestSharedProtocolOverridesAndValidation(t *testing.T) {
	o := clientOptions()
	c := clientNew(t, &clientStateDriver{}, o)
	if !reflect.DeepEqual(c.options, o) {
		t.Fatal("explicit options were overwritten")
	}
	for name, mutate := range map[string]func(*SharedOptions){
		"lease":               func(o *SharedOptions) { o.Config.LeaseTTL = -time.Second },
		"wait":                func(o *SharedOptions) { o.Config.MaxWait = -time.Second },
		"submit":              func(o *SharedOptions) { o.Config.SubmitWindow = -time.Second },
		"retention":           func(o *SharedOptions) { o.Config.Retention = -time.Second },
		"records":             func(o *SharedOptions) { o.Config.MaxRecords = -1 },
		"fractional duration": func(o *SharedOptions) { o.Config.LeaseTTL = time.Nanosecond },
		"lease bound":         func(o *SharedOptions) { o.Config.LeaseTTL = 2 * time.Hour },
		"record bound":        func(o *SharedOptions) { o.Config.MaxRecords = 10001 },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := SharedOptions{Resource: "jobs", Config: ResourceConfig{Admission: Config{Capacity: 2}}}
			mutate(&invalid)
			if _, err := NewShared(&clientStateDriver{}, invalid); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("invalid override accepted: %v", err)
			}
		})
	}
}

func TestSharedClockAllowanceValidation(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		allowance, submit, wait time.Duration
		valid                   bool
	}{
		{"zero default", 0, 0, 0, true},
		{"positive default", time.Millisecond, 0, 0, true},
		{"largest default", 2499 * time.Millisecond, 0, 0, true},
		{"half default submit", 2500 * time.Millisecond, 0, 0, false},
		{"negative", -time.Millisecond, 0, 0, false},
		{"fractional", time.Millisecond + time.Nanosecond, 0, 0, false},
		{"max duration", time.Duration(1<<63 - 1), 0, 0, false},
		{"large whole duration", time.Duration(1<<63 - 1).Truncate(time.Millisecond), 0, 0, false},
		{"below half submit", time.Millisecond, 3 * time.Millisecond, time.Second, true},
		{"half submit", time.Millisecond, 2 * time.Millisecond, time.Second, false},
		{"half wait", time.Millisecond, time.Second, 2 * time.Millisecond, false},
		{"above wait", time.Second, 5 * time.Second, time.Second, false},
		{"zero smallest windows", 0, time.Millisecond, time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := SharedOptions{Resource: "validation", Config: ResourceConfig{Admission: Config{Capacity: 1}, SubmitWindow: tc.submit, MaxWait: tc.wait}, ClockSkewAllowance: tc.allowance}
			d := &clientStateDriver{}
			c, err := NewShared(d, o)
			if tc.valid {
				if err != nil || c.options.ClockSkewAllowance != tc.allowance {
					t.Fatalf("valid allowance changed/rejected: %+v %v", c, err)
				}
			} else if !errors.Is(err, ErrInvalidConfig) || c != nil {
				t.Fatalf("invalid allowance accepted: %+v %v", c, err)
			}
			if len(d.commands()) != 0 {
				t.Fatal("constructor accessed backend")
			}
		})
	}
}

func TestSharedDefaultsDoNotMigrateExistingResource(t *testing.T) {
	d := &clientStateDriver{}
	original := clientOptions()
	previous := clientNew(t, d, original)
	lease := clientAcquire(t, previous)
	changed := clientNew(t, d, SharedOptions{
		Resource: original.Resource,
		Config:   ResourceConfig{Admission: original.Config.Admission},
	})
	if _, err := changed.Acquire(context.Background()); !errors.Is(err, ErrConfigConflict) {
		t.Fatalf("default policy silently replaced stored config: %v", err)
	}
	if err := lease.Renew(context.Background()); err != nil {
		t.Fatalf("original policy no longer works: %v", err)
	}
}

func TestSharedRejectsNonPortableTextBeforeBackend(t *testing.T) {
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
					if _, err := NewShared(d, o); !errors.Is(err, ErrInvalidConfig) {
						t.Fatalf("invalid %s accepted: %v", field, err)
					}
				})
			}
		})
	}
}

func TestSharedUnicodeNamesSurviveSnapshotRoundTrip(t *testing.T) {
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

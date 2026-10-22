package airlock

import (
	"context"
	"errors"
	"reflect"
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

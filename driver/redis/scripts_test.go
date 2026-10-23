package redis

import (
	"context"
	"testing"
	"time"
)

// Lua errors do not roll back prior writes. Assert preservation for every failed
// validation/comparison, not just the last value after a sequence of scripts.
func TestLuaCASRejectedCandidatePreservesState(t *testing.T) {
	now := time.Unix(1800000000, 123456000).UTC()
	stamp := now.UnixMicro()
	cases := []struct {
		name      string
		args      []any
		wantError bool
	}{
		{"short_arity", []any{"1", "old"}, true},
		{"long_arity", []any{"1", "old", "new", stamp, 0, "extra"}, true},
		{"invalid_existence", []any{"true", "old", "new", stamp, 0}, true},
		{"empty_candidate", []any{"1", "old", "", stamp, 0}, true},
		{"nondecimal_lower", []any{"1", "old", "new", "1e3", 0}, true},
		{"negative_upper", []any{"1", "old", "new", stamp, -1}, true},
		{"fractional_upper", []any{"1", "old", "new", stamp, "1.5"}, true},
		{"inexact_upper", []any{"1", "old", "new", stamp, "9007199254740992"}, true},
		{"empty_window", []any{"1", "old", "new", stamp, stamp}, true},
		{"expected_absent", []any{"0", "", "new", stamp, 0}, false},
		{"stale_value", []any{"1", "different", "new", stamp, 0}, false},
		{"clock_before_lower", []any{"1", "old", "new", stamp + 1, 0}, false},
		{"clock_at_exclusive_upper", []any{"1", "old", "new", stamp - 1, stamp}, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			d, client, server := fixture(t)
			server.SetTime(now)
			ctx := context.Background()
			key := d.key(test.name)
			if err := client.Set(ctx, key, "old", 0).Err(); err != nil {
				t.Fatal(err)
			}
			got, err := d.eval(ctx, casScript, key, test.args...)
			if (err != nil) != test.wantError || (!test.wantError && got != int64(0)) {
				t.Fatalf("CAS result=%v err=%v wantError=%t", got, err, test.wantError)
			}
			value, err := client.Get(ctx, key).Result()
			if err != nil || value != "old" {
				t.Fatalf("rejected candidate changed state: %q err=%v", value, err)
			}
			if ttl := client.PTTL(ctx, key).Val(); ttl != -1 {
				t.Fatalf("rejected candidate changed permanent lifetime: %v", ttl)
			}
		})
	}
}

func TestLuaCASTimeWindowEdges(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		name := "no_upper_bound"
		if bounded {
			name = "last_microsecond_before_upper"
		}
		t.Run(name, func(t *testing.T) {
			d, client, server := fixture(t)
			now := time.Unix(1800000000, 999999000).UTC()
			server.SetTime(now)
			var upper int64
			if bounded {
				upper = now.UnixMicro() + 1
			}
			key := d.key(name)
			ctx := context.Background()
			got, err := d.eval(ctx, casScript, key, "0", "", "new", now.UnixMicro(), upper)
			if err != nil || got != int64(1) {
				t.Fatalf("inclusive lower / exclusive upper: result=%v err=%v", got, err)
			}
			if value, err := client.Get(ctx, key).Result(); err != nil || value != "new" {
				t.Fatalf("committed value=%q err=%v", value, err)
			}
		})
	}
}

func TestLuaCASWrongTypePreservesOriginal(t *testing.T) {
	d, client, _ := fixture(t)
	ctx := context.Background()
	key := d.key("wrong-type")
	if err := client.RPush(ctx, key, "original").Err(); err != nil {
		t.Fatal(err)
	}
	if got, err := d.eval(ctx, casScript, key, "1", "old", "replacement", 0, 0); err == nil {
		t.Fatalf("wrong type unexpectedly accepted: %v", got)
	}
	values, err := client.LRange(ctx, key, 0, -1).Result()
	if err != nil || len(values) != 1 || values[0] != "original" {
		t.Fatalf("failed script altered list: %v err=%v", values, err)
	}
}

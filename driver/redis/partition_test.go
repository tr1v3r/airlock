package redis

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/airlock"
)

func seedPartitionHistory(t *testing.T, count int) (*Driver, airlock.Request, time.Time) {
	t.Helper()
	d, c, server := fixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	server.SetTime(now)
	r := requestAt(now)
	r.Config.MaxRecords = count + 2
	s, result, _, err := airlock.Transition(nil, now, submit(r))
	if err != nil {
		t.Fatal(err)
	}
	_ = result
	for i := 0; i < count; i++ {
		req := requestAt(now)
		req.Config = r.Config
		s.Records[req.ID] = &airlock.DriverRecord{Request: req, Status: airlock.Cancelled, Finished: now}
	}
	data, err := airlock.EncodeState(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(context.Background(), d.key(r.Resource), "state", string(data)).Err(); err != nil {
		t.Fatal(err)
	}
	return d, r, now
}

func TestPartitionMigrationAndBoundedRead(t *testing.T) {
	d, r, _ := seedPartitionHistory(t, 1000)
	ctx := context.Background()
	key := d.key(r.Resource)
	result, err := d.Observe(ctx, r)
	if err != nil || result.Status != airlock.Granted {
		t.Fatal(result, err)
	}
	c := d.client
	if c.HGet(ctx, key, "state").Val() != layoutV2 || c.ZCard(ctx, key+":history").Val() != 1000 {
		t.Fatal("missing migration")
	}
	if _, err := airlock.DecodeState([]byte(c.HGet(ctx, key, "state").Val())); err == nil {
		t.Fatal("legacy decoder accepted v2")
	}
	active, err := airlock.DecodeState([]byte(c.HGet(ctx, key, "active").Val()))
	if err != nil || len(active.Records) != 1 {
		t.Fatal(active, err)
	}
	raw, err := d.evalKeys(ctx, partitionRead, []string{key, key + ":history"}, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	fields := raw.([]any)
	if len(fields[5].([]any)) != 0 || fields[6] != int64(1000) {
		t.Fatal(fields)
	}
	id := c.ZRange(ctx, key+":history", 0, 0).Val()[0]
	retained := c.HGet(ctx, key, "r:"+id).Val()
	if _, err := d.Renew(ctx, airlock.LeaseRef{Request: r, ID: result.LeaseID}); err != nil {
		t.Fatal(err)
	}
	if c.HGet(ctx, key, "r:"+id).Val() != retained {
		t.Fatal("unrelated history rewritten")
	}
}

func TestPartitionTargetConflictAndHistoryCapacity(t *testing.T) {
	d, r, now := seedPartitionHistory(t, 3)
	ctx := context.Background()
	key := d.key(r.Resource)
	if _, err := d.Observe(ctx, r); err != nil {
		t.Fatal(err)
	}
	id := d.client.ZRange(ctx, key+":history", 0, 0).Val()[0]
	var terminal observationRecord
	if err := json.Unmarshal([]byte(d.client.HGet(ctx, key, "r:"+id).Val()), &terminal); err != nil {
		t.Fatal(err)
	}
	target := terminal.Record.Request
	target.Config = r.Config
	conflict := target
	conflict.Owner = airlock.NewRequest(now).Owner
	if result, err := d.Submit(ctx, conflict); !errors.Is(err, airlock.ErrRequestConflict) || result.Status == airlock.Granted {
		t.Fatal(result, err)
	}
	raw, err := d.evalKeys(ctx, partitionRead, []string{key, key + ":history"}, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if raw.([]any)[6] != int64(2) {
		t.Fatal("target counted twice", raw)
	}
	fresh := requestAt(now)
	fresh.Config = r.Config
	if _, err := d.Submit(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	full := requestAt(now)
	full.Config = r.Config
	if _, err := d.Submit(ctx, full); !errors.Is(err, airlock.ErrHistoryFull) {
		t.Fatal(err)
	}
}

func TestPartitionFailedPublicationRemainsUnavailable(t *testing.T) {
	d, r, _ := seedPartitionHistory(t, 2)
	ctx := context.Background()
	key := d.key(r.Resource)
	if _, err := d.Observe(ctx, r); err != nil {
		t.Fatal(err)
	}
	active := d.client.HGet(ctx, key, "active").Val()
	meta := d.client.HGet(ctx, key, "meta").Val()
	payload := `{"Meta":` + strconvQuote(meta) + `,"Records":{},"Expires":{},"Deletes":[],"Migrate":false}`
	if ok, err := d.tryUpdateLock(ctx, key, "fault"); !ok || err != nil {
		t.Fatal(ok, err)
	}
	script := strings.Replace(partitionCommit, "redis.call('HSET',KEYS[1],'state','airlock:v2','active',ARGV[2],'meta',p.Meta)", "error('injected publication failure')", 1)
	if _, err := d.evalKeys(ctx, script, []string{key, updateLockKey(key), key + ":history"}, "fault", active, 0, 0, payload); err == nil {
		t.Fatal("missing fault")
	}
	d.releaseUpdateLock(ctx, key, "fault")
	if d.client.HGet(ctx, key, "state").Val() != "airlock:v2:incomplete" {
		t.Fatal("lost guard")
	}
	if _, err := d.Observe(ctx, r); err == nil {
		t.Fatal("observed partial publication")
	}
	if _, err := d.Submit(ctx, r); err == nil {
		t.Fatal("wrote over partial publication")
	}
	if _, err := d.InspectResource(ctx, r.Resource); err == nil {
		t.Fatal("inspected partial publication")
	}
	// The old reader also sees no ready projection and its snapshot cannot decode.
	raw, err := d.eval(ctx, observeScript, key, r.ID)
	if err != nil || raw.([]any)[0] != "" {
		t.Fatal(raw, err)
	}
}

func strconvQuote(s string) string { b, _ := json.Marshal(s); return string(b) }

func TestPartitionWrongIndexTypePreservesState(t *testing.T) {
	d, r, _ := seedPartitionHistory(t, 1)
	ctx := context.Background()
	key := d.key(r.Resource)
	before := d.client.HGetAll(ctx, key).Val()
	if err := d.client.Set(ctx, key+":history", "wrong", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Submit(ctx, r); err == nil {
		t.Fatal("accepted wrong index type")
	}
	if !reflect.DeepEqual(before, d.client.HGetAll(ctx, key).Val()) {
		t.Fatal("modified before preflight")
	}
}

func TestPartitionMalformedReadFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		change func([]any) []any
	}{
		{"kind", func(v []any) []any { v[0] = int64(8); return v }},
		{"time", func(v []any) []any { v[2] = "bad"; return v }},
		{"snapshot", func(v []any) []any { v[1] = "{"; return v }},
		{"short", func(v []any) []any { return v[:8] }},
		{"header", func(v []any) []any { v[4] = "{}"; return v }},
		{"count", func(v []any) []any { v[6] = int64(-1); return v }},
		{"before", func(v []any) []any { v[7] = "bad"; return v }},
		{"terminal", func(v []any) []any { v[5] = []any{"bad", "{}", "1"}; return v }},
		{"score", func(v []any) []any { r := v[5].([]any); r[2] = "1"; return v }},
		{"nil-record", func(v []any) []any {
			var s airlock.SharedState
			_ = json.Unmarshal([]byte(v[1].(string)), &s)
			for id := range s.Records {
				s.Records[id] = nil
			}
			b, _ := json.Marshal(&s)
			v[1] = string(b)
			return v
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			d, r, _ := seedPartitionHistory(t, 1)
			ctx := context.Background()
			if _, err := d.Observe(ctx, r); err != nil {
				t.Fatal(err)
			}
			key := d.key(r.Resource)
			id := d.client.ZRange(ctx, key+":history", 0, 0).Val()[0]
			var rec observationRecord
			_ = json.Unmarshal([]byte(d.client.HGet(ctx, key, "r:"+id).Val()), &rec)
			target := rec.Record.Request
			target.Config = r.Config
			d.client.(*goredis.Client).AddHook(hook{func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
				err := next(ctx, cmd)
				if isScript(cmd, partitionRead) && err == nil {
					cmd.(*noRetryCmd).SetVal(tt.change(cmd.(*noRetryCmd).Val().([]any)))
				}
				return err
			}})
			if result, err := d.Submit(ctx, target); err == nil || result != (airlock.Result{}) {
				t.Fatal(result, err)
			}
		})
	}
}

func TestPartitionPublicationAndLegacyIndexErrors(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := requestAt(now)
	state, _, _, err := airlock.Transition(nil, now, submit(r))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index(state); err != nil {
		t.Fatal(err)
	}
	state.Records[r.ID] = nil
	if _, _, err := partitionPublication(state, airlock.RetainedHistory{}, "token", nil, false); err == nil {
		t.Fatal("accepted null record")
	}
	state.Records = map[string]*airlock.DriverRecord{}
	state.Time = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, _, err := partitionPublication(state, airlock.RetainedHistory{}, "token", nil, false); err == nil {
		t.Fatal("accepted invalid JSON time")
	}
	state.Time = now
	bad := r
	bad.SubmitUntil = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	state.Records[r.ID] = &airlock.DriverRecord{Request: bad, Status: airlock.Granted, Until: now.Add(time.Second)}
	if _, _, err := partitionPublication(state, airlock.RetainedHistory{}, "token", nil, false); err == nil {
		t.Fatal("accepted invalid record time")
	}
}

func TestPartitionAllDueHistoryReclaimedWithoutFalseFull(t *testing.T) {
	d, c, server := fixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	server.SetTime(now)
	r := requestAt(now)
	r.Config.MaxRecords = 80
	state, _, _, err := airlock.Transition(nil, now, submit(r))
	if err != nil {
		t.Fatal(err)
	}
	state.Records = map[string]*airlock.DriverRecord{}
	var target airlock.Request
	for i := 0; i < 80; i++ {
		target = requestAt(now)
		target.Config = r.Config
		state.Records[target.ID] = &airlock.DriverRecord{Request: target, Status: airlock.Cancelled, Finished: now}
	}
	encoded, err := airlock.EncodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	key := d.key(r.Resource)
	if err := c.HSet(ctx, key, "state", string(encoded)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Resize(ctx, airlock.ResizeRequest{Resource: r.Resource, Capacity: 1, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	later := target.SubmitUntil.Add(r.Config.Retention + time.Second)
	server.SetTime(later)
	conflict := target
	conflict.Owner = airlock.NewRequest(later).Owner
	if _, err := d.Submit(ctx, conflict); !errors.Is(err, airlock.ErrRequestConflict) {
		t.Fatal("due identity conflict lost", err)
	}
	fresh := requestAt(later)
	fresh.Config = r.Config
	result, err := d.Submit(ctx, fresh)
	if err != nil || result.Status != airlock.Granted {
		t.Fatal("false history full", result, err)
	}
	if n := c.ZCard(ctx, key+":history").Val(); n != 0 {
		t.Fatal("due history retained", n)
	}
	if n := c.HLen(ctx, key).Val(); n != 4 {
		t.Fatal("orphan history", n)
	}
}

func TestPartitionCommitGuard(t *testing.T) {
	now := time.Unix(1800000000, 123456000).UTC()
	stamp := now.UnixMicro()
	for _, tc := range []struct {
		name         string
		lower, upper any
		owner        string
		want         int64
		wantError    bool
	}{
		{"lower", stamp + 1, 0, "owner", 0, false}, {"upper", stamp - 1, stamp, "owner", 0, false},
		{"owner", stamp, 0, "lost", 0, false}, {"fraction", stamp, "1.5", "owner", 0, true},
		{"exact", stamp, stamp + 1, "owner", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, c, server := fixture(t)
			server.SetTime(now)
			ctx := context.Background()
			key := d.key(tc.name)
			if ok, err := d.tryUpdateLock(ctx, key, "owner"); !ok || err != nil {
				t.Fatal(ok, err)
			}
			payload := `{"Meta":"meta","Records":{},"Expires":{},"Deletes":[],"Migrate":true}`
			result, err := d.evalKeys(ctx, partitionCommit, []string{key, updateLockKey(key), key + ":history"}, tc.owner, "snapshot", tc.lower, tc.upper, payload)
			if (err != nil) != tc.wantError || (!tc.wantError && result != tc.want) {
				t.Fatal(result, err)
			}
			if tc.want != 1 && c.Exists(ctx, key).Val() != 0 {
				t.Fatal("rejected candidate mutated state")
			}
		})
	}
}

func integrationPartitionHistory(t *testing.T, client goredis.UniversalClient, namespace string) {
	t.Helper()
	ctx := context.Background()
	d, err := New(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	r := requestAt(now)
	r.Config.MaxRecords = 1002
	state, _, _, err := airlock.Transition(nil, now, submit(r))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		req := requestAt(now)
		req.Config = r.Config
		state.Records[req.ID] = &airlock.DriverRecord{Request: req, Status: airlock.Cancelled, Finished: now}
	}
	data, err := airlock.EncodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	key := d.key(r.Resource)
	t.Cleanup(func() { client.Del(context.Background(), key, key+":history", updateLockKey(key)) })
	if err := client.HSet(ctx, key, "state", string(data)).Err(); err != nil {
		t.Fatal(err)
	}
	result, err := d.Observe(ctx, r)
	if err != nil || result.Status != airlock.Granted {
		t.Fatal(result, err)
	}
	if client.ZCard(ctx, key+":history").Val() != 1000 {
		t.Fatal("migration lost history")
	}
	raw, err := d.evalKeys(ctx, partitionRead, []string{key, key + ":history"}, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw.([]any)[5].([]any)) != 0 || raw.([]any)[6] != int64(1000) {
		t.Fatal("non-due history loaded", raw)
	}
	if _, err := d.Renew(ctx, airlock.LeaseRef{Request: r, ID: result.LeaseID}); err != nil {
		t.Fatal(err)
	}
}

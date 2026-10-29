package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestDeltaProjectionReusesUnchangedRecordsAndRepairsLegacyCandidate(t *testing.T) {
	d, c, server := fixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	server.SetTime(now)
	r := requestAt(now)
	held := mustExecute(t, d, submit(r))
	pending := requestAt(now)
	mustExecute(t, d, submit(pending))
	key := d.key(r.Resource)
	before := c.HGet(ctx, key, "r:"+pending.ID).Val()
	meta := c.HGet(ctx, key, "meta").Val()
	var header observationHeader
	if err := json.Unmarshal([]byte(meta), &header); err != nil {
		t.Fatal(err)
	}
	server.SetTime(now.Add(time.Millisecond))
	mustExecute(t, d, airlock.Command{Kind: airlock.Renew, Request: r, LeaseID: held.LeaseID})
	if got := c.HGet(ctx, key, "r:"+pending.ID).Val(); got != before {
		t.Fatal("renew rewrote unchanged pending projection")
	}
	var after observationHeader
	_ = json.Unmarshal([]byte(c.HGet(ctx, key, "meta").Val()), &after)
	if after.Token != header.Token {
		t.Fatal("delta changed generation token")
	}
	// An interrupted old full writer may have staged a different generation.
	var staged observationRecord
	_ = json.Unmarshal([]byte(before), &staged)
	staged.Token = "unpublished-legacy-candidate"
	staged.Record.Status = airlock.Granted
	staged.Record.LeaseID = "not-authoritative"
	raw, _ := json.Marshal(staged)
	if err := c.HSet(ctx, key, "r:"+pending.ID, raw).Err(); err != nil {
		t.Fatal(err)
	}
	server.SetTime(now.Add(2 * time.Millisecond))
	mustExecute(t, d, airlock.Command{Kind: airlock.Renew, Request: r, LeaseID: held.LeaseID})
	if result, ready, err := d.observe(ctx, pending); ready || err != nil {
		t.Fatalf("staged candidate authorized: %+v ready=%v err=%v", result, ready, err)
	}
	// Observe falls back to a full publication, restoring the authoritative queue.
	result, err := d.Observe(ctx, pending)
	if err != nil || result.Status != airlock.Queued {
		t.Fatal(result, err)
	}
	_ = json.Unmarshal([]byte(c.HGet(ctx, key, "meta").Val()), &after)
	if after.Token == header.Token {
		t.Fatal("full repair did not change generation")
	}
	before = c.HGet(ctx, key, "r:"+pending.ID).Val()
	server.SetTime(now.Add(3 * time.Millisecond))
	mustExecute(t, d, airlock.Command{Kind: airlock.Renew, Request: r, LeaseID: held.LeaseID})
	if c.HGet(ctx, key, "r:"+pending.ID).Val() != before {
		t.Fatal("delta could not follow full legacy-compatible publication")
	}
}

func deltaFixture(count int) (*airlock.SharedState, map[string]recordVersion) {
	now := time.Unix(1800000000, 0).UTC()
	r := requestAt(now)
	s := &airlock.SharedState{Version: 1, Resource: r.Resource, Config: r.Config, Time: now, Revision: 1, Capacity: 1, CapacityVersion: 1, Records: make(map[string]*airlock.DriverRecord)}
	before := make(map[string]recordVersion, count)
	for i := 0; i < count; i++ {
		rec := &airlock.DriverRecord{Request: r, Status: airlock.Cancelled, Finished: now}
		rec.Request.ID = fmt.Sprint(i)
		s.Records[rec.Request.ID] = rec
		before[rec.Request.ID] = version(rec)
	}
	return s, before
}

func TestDeltaThresholdAndAtomicPublication(t *testing.T) {
	for _, count := range []int{0, 256, 257} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			d, c, _ := fixture(t)
			s, before := deltaFixture(count)
			for _, rec := range s.Records {
				rec.Finished = rec.Finished.Add(time.Millisecond)
			}
			data, err := deltaIndex(s, "generation", before)
			if err != nil {
				t.Fatal(err)
			}
			if count == 257 {
				if data != nil {
					t.Fatal("oversized delta accepted")
				}
				return
			}
			if data == nil {
				t.Fatal("bounded delta rejected")
			}
			key := d.key(s.Resource)
			ctx := context.Background()
			if ok, err := d.tryUpdateLock(ctx, key, "owner"); !ok || err != nil {
				t.Fatal(ok, err)
			}
			got, err := d.evalKeys(ctx, deltaCommitScript, []string{key, updateLockKey(key)}, "owner", "snapshot", 0, 0, string(data))
			if err != nil || got != int64(1) {
				t.Fatal(got, err)
			}
			if c.HGet(ctx, key, "state").Val() != "snapshot" || c.HLen(ctx, key).Val() != int64(2+count) {
				t.Fatal("incomplete delta")
			}
		})
	}
}

func TestDeltaValidationBeforeAnyWrites(t *testing.T) {
	d, c, _ := fixture(t)
	ctx := context.Background()
	key := d.key("guards")
	c.HSet(ctx, key, "state", "old", "meta", "old-meta", "r:delete", "old-record")
	if ok, err := d.tryUpdateLock(ctx, key, "owner"); !ok || err != nil {
		t.Fatal(ok, err)
	}
	for _, bad := range []string{
		`{"Meta":"new","Records":{},"Deletes":null}`,
		`{"Meta":"new","Records":{},"Deletes":["delete",12]}`,
		`{"Meta":"new","Records":{},"Deletes":[` + strings.TrimSuffix(strings.Repeat(`"delete",`, 257), ",") + `]}`,
	} {
		if _, err := d.evalKeys(ctx, deltaCommitScript, []string{key, updateLockKey(key)}, "owner", "new", 0, 0, bad); err == nil {
			t.Fatal("invalid delta accepted")
		}
		if c.HGet(ctx, key, "r:delete").Val() != "old-record" || c.HGet(ctx, key, "state").Val() != "old" {
			t.Fatal("validation changed state")
		}
	}
}

func TestProjectionBaselineRejectsMismatchedHeader(t *testing.T) {
	s, _ := deltaFixture(1)
	header := observationHeader{Observation: airlock.Observation{Resource: s.Resource, Revision: s.Revision, Time: s.Time}, Token: "generation"}
	raw := func(h observationHeader) []any {
		b, _ := json.Marshal(h)
		return []any{int64(1), "snapshot", "1800000000", "0", string(b), int64(len(s.Records) + 2)}
	}
	if token, before := projectionBaseline(s, raw(header)); token != "generation" || len(before) != 1 {
		t.Fatal(token, before)
	}
	for _, mode := range []string{"revision", "time", "resource", "token"} {
		h := header
		switch mode {
		case "revision":
			h.Revision++
		case "time":
			h.Time = h.Time.Add(time.Millisecond)
		case "resource":
			h.Resource = "other"
		case "token":
			h.Token = ""
		}
		if token, _ := projectionBaseline(s, raw(h)); token != "" {
			t.Fatal("accepted", mode)
		}
	}
	for _, bad := range []any{nil, []any{}, []any{0, 0, 0, 0, 42, int64(3)}, []any{0, 0, 0, 0, "{", int64(3)}, []any{0, 0, 0, 0, "{}", int64(4)}, []any{0, 0, 0, 0, "{}", "3"}} {
		if token, _ := projectionBaseline(s, bad); token != "" {
			t.Fatal("accepted bad header")
		}
	}
	s.Records["0"] = nil
	if token, _ := projectionBaseline(s, raw(header)); token != "" {
		t.Fatal("accepted nil record")
	}
	if token, _ := projectionBaseline(nil, nil); token != "" {
		t.Fatal(token)
	}
}

func BenchmarkProjectionUpdate(b *testing.B) {
	for _, count := range []int{1, 1000, 10000} {
		s, _ := deltaFixture(count)
		o, _ := s.Observation()
		meta, _ := json.Marshal(observationHeader{Observation: o, Token: "generation"})
		raw := []any{int64(1), "snapshot", "1800000000", "0", string(meta), int64(len(s.Records) + 2)}
		// Index cost includes baseline allocation in the delta case, as execute does.
		for _, mode := range []string{"full", "delta"} {
			b.Run(fmt.Sprintf("%d/%s", count, mode), func(b *testing.B) {
				b.ReportAllocs()
				var size int
				for i := 0; i < b.N; i++ {
					var data []byte
					var err error
					if mode == "full" {
						data, err = index(s)
					} else {
						token, before := projectionBaseline(s, raw)
						before["0"] = recordVersion{}
						data, err = deltaIndex(s, token, before)
					}
					if err != nil {
						b.Fatal(err)
					}
					size = len(data)
				}
				b.ReportMetric(float64(size), "projection_bytes")
			})
		}
	}
}

func TestDeltaFailureAfterDeletionFallsBackToCommittedSnapshot(t *testing.T) {
	d, c, _ := fixture(t)
	ctx := context.Background()
	r := request()
	mustExecute(t, d, submit(r))
	pending := request()
	mustExecute(t, d, submit(pending))
	mustExecute(t, d, airlock.Command{Kind: airlock.Cancel, Request: pending})
	key := d.key(r.Resource)
	oldState := c.HGet(ctx, key, "state").Val()
	oldMeta := c.HGet(ctx, key, "meta").Val()
	data, _ := json.Marshal(struct {
		Meta    string
		Records map[string]string
		Deletes []string
	}{"not-published", map[string]string{}, []string{pending.ID}})
	if ok, err := d.tryUpdateLock(ctx, key, "fault-owner"); !ok || err != nil {
		t.Fatal(ok, err)
	}
	// Inject the failure precisely after deletion and before final publication.
	failing := strings.Replace(deltaCommitScript, "redis.call('HSET', KEYS[1], unpack(args))", "error('injected publication failure')", 1)
	if _, err := d.evalKeys(ctx, failing, []string{key, updateLockKey(key)}, "fault-owner", "not-published", 0, 0, string(data)); err == nil {
		t.Fatal("expected injected failure")
	}
	d.releaseUpdateLock(ctx, key, "fault-owner")
	if c.HGet(ctx, key, "state").Val() != oldState || c.HGet(ctx, key, "meta").Val() != oldMeta {
		t.Fatal("failed delta published state")
	}
	if c.HExists(ctx, key, "r:"+pending.ID).Val() {
		t.Fatal("failure was not after deletion")
	}
	result, err := d.Observe(ctx, pending)
	if err != nil || result.Status != airlock.Cancelled {
		t.Fatal(result, err)
	}
	if !c.HExists(ctx, key, "r:"+pending.ID).Val() {
		t.Fatal("missing projection not repaired")
	}
}

func TestIntegrationDeltaLuaArgumentBoundary(t *testing.T) {
	client := integrationClient(t, false)
	ctx := context.Background()
	d, err := New(client, "delta-boundary-"+airlock.NewRequest(time.Now()).Owner)
	if err != nil {
		t.Fatal(err)
	}
	state, before := deltaFixture(256)
	for _, rec := range state.Records {
		rec.Finished = rec.Finished.Add(time.Millisecond)
	}
	data, err := deltaIndex(state, "generation", before)
	if err != nil || data == nil {
		t.Fatal(err)
	}
	key := d.key(state.Resource)
	defer client.Del(ctx, key, updateLockKey(key))
	if ok, err := d.tryUpdateLock(ctx, key, "owner"); !ok || err != nil {
		t.Fatal(ok, err)
	}
	result, err := d.evalKeys(ctx, deltaCommitScript, []string{key, updateLockKey(key)}, "owner", "snapshot", 0, 0, string(data))
	if err != nil || result != int64(1) {
		t.Fatal(result, err)
	}
	if fields, err := client.HLen(ctx, key).Result(); err != nil || fields != 258 {
		t.Fatal(fields, err)
	}
}

package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tr1v3r/airlock"
)

func TestProjectionReusesUnchangedRecordsAndRepairsUnpublishedCandidate(t *testing.T) {
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
	// A corrupt projection with an unpublished generation cannot authorize work.
	var staged observationRecord
	_ = json.Unmarshal([]byte(before), &staged)
	staged.Token = "unpublished-candidate"
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
	if after.Token != header.Token {
		t.Fatal("partition repair changed generation of unrelated history")
	}
	before = c.HGet(ctx, key, "r:"+pending.ID).Val()
	server.SetTime(now.Add(3 * time.Millisecond))
	mustExecute(t, d, airlock.Command{Kind: airlock.Renew, Request: r, LeaseID: held.LeaseID})
	if c.HGet(ctx, key, "r:"+pending.ID).Val() != before {
		t.Fatal("delta could not follow partition publication")
	}
}

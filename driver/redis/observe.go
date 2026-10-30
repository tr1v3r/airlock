package redis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/tr1v3r/airlock"
)

type observationHeader struct {
	airlock.Observation
	Token string
}

type observationRecord struct {
	Token  string
	Record *airlock.DriverRecord
}

func (d *Driver) observe(ctx context.Context, request airlock.Request) (airlock.Result, bool, error) {
	raw, err := d.eval(ctx, observeScript, d.key(request.Resource), request.ID)
	if err != nil {
		return airlock.Result{}, true, err
	}

	values, ok := raw.([]any)
	if !ok || len(values) != 4 {
		return airlock.Result{}, true, errors.New("airlock/redis: malformed observation")
	}

	meta, okMeta := values[0].(string)
	record, okRecord := values[1].(string)
	if !okMeta || !okRecord {
		return airlock.Result{}, true, errors.New("airlock/redis: malformed observation fields")
	}

	// Reuse backend-time validation, without transferring a state snapshot.
	_, now, err := decodeRead([]any{int64(0), "", values[2], values[3]})
	if err != nil {
		return airlock.Result{}, true, err
	}

	if meta == "" || record == "" {
		return airlock.Result{}, false, nil
	}

	var o observationHeader
	var rec observationRecord
	if json.Unmarshal([]byte(meta), &o) != nil || json.Unmarshal([]byte(record), &rec) != nil {
		return airlock.Result{}, true, airlock.ErrCorruptState
	}

	if rec.Token == "" || rec.Token != o.Token {
		return airlock.Result{}, false, nil
	}

	o.Record = rec.Record
	result, ready, err := o.Observe(now, request)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return airlock.Result{}, true, ctxErr
	}
	return result, ready, err
}

// recordVersion contains every field Transition can change on an existing record.
// Request identity is immutable; Config is deliberately restored/cleared by the
// reference state machine and is not a stored per-record change.
type recordVersion struct {
	Status          airlock.Status
	LeaseID         string
	Until, Finished time.Time
	Failure         string
}

func version(rec *airlock.DriverRecord) recordVersion {
	return recordVersion{rec.Status, rec.LeaseID, rec.Until, rec.Finished, rec.Failure}
}

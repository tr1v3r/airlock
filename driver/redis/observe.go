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

type observationIndex struct {
	Meta    string
	Records map[string]string
}

// index builds the read projection once per transition. Every request is tagged
// with the same random publication token as the header, so partial preparation
// cannot expose a request from an uncommitted candidate.
func index(state *airlock.SharedState) ([]byte, error) {
	token := airlock.NewRequest(state.Time).Owner

	observation, err := state.Observation()
	if err != nil {
		return nil, err
	}
	meta, err := json.Marshal(observationHeader{
		Observation: observation,
		Token:       token,
	})
	if err != nil {
		return nil, err
	}

	i := observationIndex{
		Meta:    string(meta),
		Records: make(map[string]string, len(state.Records)),
	}

	for id, rec := range state.Records {
		data, err := json.Marshal(observationRecord{Token: token, Record: rec})
		if err != nil {
			return nil, err
		}

		i.Records[id] = string(data)
	}

	return json.Marshal(i)
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

func projectionBaseline(state *airlock.SharedState, raw any) (string, map[string]recordVersion) {
	if state == nil {
		return "", nil
	}
	values, ok := raw.([]any)
	if !ok || len(values) != 6 {
		return "", nil
	}
	// A constant-cost HLEN check detects missing/orphaned legacy projections
	// even during a pure mutation stream; rebuild instead of preserving them.
	fields, ok := values[5].(int64)
	if !ok || fields != int64(len(state.Records))+2 {
		return "", nil
	}
	meta, ok := values[4].(string)
	if !ok {
		return "", nil
	}
	var header observationHeader
	if json.Unmarshal([]byte(meta), &header) != nil || header.Token == "" ||
		header.Resource != state.Resource || header.Revision != state.Revision || !header.Time.Equal(state.Time) {
		return "", nil
	}
	before := make(map[string]recordVersion, len(state.Records))
	for id, rec := range state.Records {
		if rec == nil {
			return "", nil
		}
		before[id] = version(rec)
	}
	return header.Token, before
}

// deltaIndex returns nil when a bounded atomic delta cannot be used. Observe
// maintenance uses a full rebuild to repair missing/partially prepared indexes.
func deltaIndex(state *airlock.SharedState, token string, before map[string]recordVersion) ([]byte, error) {
	if token == "" {
		return nil, nil
	}
	changed := make(map[string]*airlock.DriverRecord)
	deleted := make([]string, 0)
	for id, rec := range state.Records {
		if old, ok := before[id]; !ok || old != version(rec) {
			changed[id] = rec
		}
		if len(changed) > 256 {
			return nil, nil
		}
	}
	for id := range before {
		if state.Records[id] == nil {
			deleted = append(deleted, id)
		}
		if len(changed)+len(deleted) > 256 {
			return nil, nil
		}
	}
	o, err := state.Observation()
	if err != nil {
		return nil, err
	}
	meta, err := json.Marshal(observationHeader{Observation: o, Token: token})
	if err != nil {
		return nil, err
	}
	// Deletes must be a JSON array even when empty, because Lua validates it.
	i := struct {
		Meta    string
		Records map[string]string
		Deletes []string
	}{string(meta), make(map[string]string, len(changed)), deleted}
	for id, rec := range changed {
		data, err := json.Marshal(observationRecord{Token: token, Record: rec})
		if err != nil {
			return nil, err
		}
		i.Records[id] = string(data)
	}
	return json.Marshal(i)
}

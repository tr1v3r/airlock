package redis

import (
	"context"
	"encoding/json"
	"errors"

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

	meta, err := json.Marshal(observationHeader{
		Observation: airlock.Observation{
			Resource:        state.Resource,
			Config:          state.Config,
			Time:            state.Time,
			Revision:        state.Revision,
			NextMaintenance: state.NextMaintenance(),
		},
		Token: token,
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
	_, _, now, err := decodeRead([]any{int64(0), "", values[2], values[3]})
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

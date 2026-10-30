package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/tr1v3r/airlock"
)

func (d *Driver) executePartitioned(ctx context.Context, command airlock.Command, key, owner string) (airlock.Result, bool, error) {
	raw, err := d.evalKeys(ctx, partitionRead, []string{key, key + ":history"}, command.Request.ID)
	if err != nil {
		return airlock.Result{}, false, err
	}
	values, ok := raw.([]any)
	if !ok || (len(values) != 8 && len(values) != 10) {
		return airlock.Result{}, false, airlock.ErrCorruptState
	}
	kind, ok := values[0].(int64)
	if !ok || kind < 0 || kind > 1 {
		return airlock.Result{}, false, airlock.ErrCorruptState
	}
	snapshot, now, err := decodeRead([]any{kind, values[1], values[2], values[3]})
	if err != nil {
		return airlock.Result{}, false, err
	}
	state, err := airlock.DecodeState(snapshot)
	if err != nil {
		return airlock.Result{}, false, err
	}
	history := airlock.RetainedHistory{}
	token := ""
	if kind == 1 {
		if state == nil || len(values) != 10 {
			return airlock.Result{}, false, airlock.ErrCorruptState
		}
		for _, rec := range state.Records {
			if rec == nil || !activeRecord(rec) {
				return airlock.Result{}, false, airlock.ErrCorruptState
			}
		}
		fields, fok := values[8].(int64)
		total, tok := values[9].(int64)
		if !fok || !tok || fields != int64(len(state.Records))+total+2 {
			return airlock.Result{}, false, airlock.ErrCorruptState
		}
		meta, mok := values[4].(string)
		var header observationHeader
		if !mok || json.Unmarshal([]byte(meta), &header) != nil || header.Token == "" || header.Revision != state.Revision || header.Resource != state.Resource || !header.Time.Equal(state.Time) {
			return airlock.Result{}, false, airlock.ErrCorruptState
		}
		token = header.Token
		records, rok := values[5].([]any)
		count, cok := values[6].(int64)
		before, bok := values[7].(string)
		if !rok || !cok || !bok || len(records)%3 != 0 || count < 0 || count > int64(state.Config.MaxRecords) {
			return airlock.Result{}, false, airlock.ErrCorruptState
		}
		history.Count = int(count)
		if before != "" {
			stamp, e := strconv.ParseInt(before, 10, 64)
			if e != nil {
				return airlock.Result{}, false, e
			}
			history.Before = time.UnixMilli(stamp).UTC()
		}
		for i := 0; i < len(records); i += 3 {
			id, iok := records[i].(string)
			encoded, eok := records[i+1].(string)
			var rec observationRecord
			if !iok || !eok || json.Unmarshal([]byte(encoded), &rec) != nil || rec.Token != token || rec.Record == nil || rec.Record.Request.ID != id || activeRecord(rec.Record) {
				return airlock.Result{}, false, airlock.ErrCorruptState
			}
			score, sok := records[i+2].(string)
			stamp, scoreErr := strconv.ParseInt(score, 10, 64)
			at := rec.Record.Finished.Add(state.Config.Retention)
			if rec.Record.Request.SubmitUntil.After(at) {
				at = rec.Record.Request.SubmitUntil
			}
			if !sok || scoreErr != nil || stamp != at.UnixMilli() {
				return airlock.Result{}, false, airlock.ErrCorruptState
			}
			state.Records[id] = rec.Record
		}
	}
	before := make(map[string]recordVersion)
	if state != nil {
		for id, rec := range state.Records {
			before[id] = version(rec)
		}
	}
	next, result, window, transitionErr := airlock.TransitionPartial(state, now, command, history)
	if next == nil {
		return result, false, transitionErr
	}
	publicationBaseline := before
	if command.Kind == airlock.Observe {
		// Repair loaded projections without rewriting unrelated retained history.
		publicationBaseline = make(map[string]recordVersion, len(before))
		for id := range before {
			publicationBaseline[id] = recordVersion{}
		}
	}
	projection, active, err := partitionPublication(next, history, token, publicationBaseline)
	if err != nil {
		return airlock.Result{}, false, err
	}
	lower, err := micro(window.NotBefore)
	if err != nil {
		return airlock.Result{}, false, err
	}
	upper, err := micro(window.Before)
	if err != nil {
		return airlock.Result{}, false, err
	}
	raw, err = d.evalKeys(ctx, partitionCommit, []string{key, updateLockKey(key), key + ":history"}, owner, string(active), lower, upper, string(projection))
	if err != nil {
		return airlock.Result{}, false, err
	}
	committed, ok := raw.(int64)
	if !ok || (committed != 0 && committed != 1) {
		return airlock.Result{}, false, errors.New("airlock/redis: malformed commit response")
	}
	return result, committed == 0, transitionErr
}

func activeRecord(rec *airlock.DriverRecord) bool {
	return rec.Status == airlock.Queued || rec.Status == airlock.Granted
}

func partitionPublication(state *airlock.SharedState, history airlock.RetainedHistory, token string, before map[string]recordVersion) ([]byte, []byte, error) {
	if token == "" {
		token = airlock.NewRequest(state.Time).Owner
	}
	observation, err := state.Observation()
	if err != nil {
		return nil, nil, err
	}
	if !history.Before.IsZero() && (observation.NextMaintenance.IsZero() || history.Before.Before(observation.NextMaintenance)) {
		observation.NextMaintenance = history.Before
	}
	meta, err := json.Marshal(observationHeader{Observation: observation, Token: token})
	if err != nil {
		return nil, nil, err
	}
	p := struct {
		Meta    string
		Records map[string]string
		Expires map[string]int64
		Deletes []string
	}{string(meta), map[string]string{}, map[string]int64{}, []string{}}
	active := *state
	active.Records = make(map[string]*airlock.DriverRecord)
	for id, rec := range state.Records {
		if activeRecord(rec) {
			active.Records[id] = rec
		}
		if old, ok := before[id]; ok && old == version(rec) {
			continue
		}
		encoded, e := json.Marshal(observationRecord{Token: token, Record: rec})
		if e != nil {
			return nil, nil, e
		}
		p.Records[id] = string(encoded)
		if !activeRecord(rec) {
			at := rec.Finished.Add(state.Config.Retention)
			if rec.Request.SubmitUntil.After(at) {
				at = rec.Request.SubmitUntil
			}
			p.Expires[id] = at.UnixMilli()
		}
	}
	for id := range before {
		if state.Records[id] == nil {
			p.Deletes = append(p.Deletes, id)
		}
	}
	encoded, err := airlock.EncodeState(&active)
	if err != nil {
		return nil, nil, err
	}
	publication, err := json.Marshal(p)
	return publication, encoded, err
}

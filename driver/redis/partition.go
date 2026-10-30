package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/tr1v3r/airlock"
)

const layoutV2 = "airlock:v2"

// partitionRead reads all active state, the target and expired history only.
// The expiry index and hash are authoritative together; incomplete publications
// remain unavailable rather than silently recreating a resource.
const partitionRead = `
local marker = redis.call('HGET', KEYS[1], 'state')
local tm = redis.call('TIME')
if not marker then
 if redis.call('EXISTS', KEYS[1]) ~= 0 or redis.call('EXISTS', KEYS[2]) ~= 0 then return redis.error_reply('incomplete resource') end
 return {0, '', tm[1], tm[2], '', {}, 0, ''}
end
if marker ~= 'airlock:v2' then
 if string.sub(marker,1,8) == 'airlock:' then return redis.error_reply('incomplete layout') end
 return {1,marker,tm[1],tm[2],'',{},0,''}
end
local active = redis.call('HGET',KEYS[1],'active')
local meta = redis.call('HGET',KEYS[1],'meta')
if not active or not meta or meta == '' then return redis.error_reply('incomplete resource') end
local now = tonumber(tm[1])*1000+math.floor(tonumber(tm[2])/1000)
local due = redis.call('ZRANGEBYSCORE',KEYS[2],'-inf',now)
local records = {}
local seen = {}
for _,id in ipairs(due) do seen[id]=true end
if ARGV[1] ~= '' and not seen[ARGV[1]] and redis.call('ZSCORE',KEYS[2],ARGV[1]) then due[#due+1]=ARGV[1] end
for _,id in ipairs(due) do
 local rec = redis.call('HGET',KEYS[1],'r:'..id)
 if not rec then return redis.error_reply('missing terminal') end
 records[#records+1]=id; records[#records+1]=rec; records[#records+1]=redis.call('ZSCORE',KEYS[2],id)
end
local count = redis.call('ZCARD',KEYS[2])-redis.call('ZCOUNT',KEYS[2],'-inf',now)
local targetScore = redis.call('ZSCORE',KEYS[2],ARGV[1])
if targetScore and tonumber(targetScore)>now then count=count-1 end
local first = redis.call('ZRANGEBYSCORE',KEYS[2],'('..now,'+inf','WITHSCORES','LIMIT',0,2)
local before = ''
for i=1,#first,2 do if first[i] ~= ARGV[1] then before=first[i+1]; break end end
return {2,active,tm[1],tm[2],meta,records,count,before,redis.call('HLEN',KEYS[1]),redis.call('ZCARD',KEYS[2])}
`

// partitionCommit deliberately publishes an unavailable marker before any
// multi-command mutation. Redis scripts do not roll back after runtime errors.
// A failed publication can never expose partial capacity/history as valid.
const partitionCommit = `
	if #KEYS ~= 3 or #ARGV ~= 5 then return redis.error_reply('invalid arguments') end
	if #ARGV[1] == 0 then return redis.error_reply('empty lock owner') end
	if #ARGV[2] == 0 then return redis.error_reply('empty snapshot') end
	-- Ownership is checked in the same atomic operation that publishes state.
	-- A paused process cannot commit after expiry, even without a successor.
	if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end

	local function integer(s)
		if not string.match(s, '^%d+$') then return nil end
		local n = tonumber(s)
		if not n or n > 9007199254740991 or n < 0 or n ~= math.floor(n) then return nil end
		return n
	end

	local lower = integer(ARGV[3])
	local upper = integer(ARGV[4])
	if not lower or not upper or (upper ~= 0 and upper <= lower) then
		return redis.error_reply('invalid window')
	end

	local tm = redis.call('TIME')
	local sec = integer(tm[1])
	local us = integer(tm[2])
	if not sec or not us or us > 999999 or sec > 9007199254 then
		return redis.error_reply('invalid clock')
	end

	local now = sec * 1000000 + us
	if now > 9007199254740991 then return redis.error_reply('invalid clock') end
	if now < lower or (upper ~= 0 and now >= upper) then return 0 end

local p=cjson.decode(ARGV[5])
if type(p)~='table' or type(p.Meta)~='string' or type(p.Records)~='table' or type(p.Expires)~='table' or type(p.Deletes)~='table' or type(p.Migrate)~='boolean' or ARGV[2]=='' then return redis.error_reply('invalid publication') end
for id,value in pairs(p.Records) do if type(id)~='string' or type(value)~='string' then return redis.error_reply('invalid record') end end
for id,score in pairs(p.Expires) do if type(id)~='string' or type(score)~='number' or score<=0 or score>9007199254740991 or score~=math.floor(score) or not p.Records[id] then return redis.error_reply('invalid expiry') end end
for _,id in ipairs(p.Deletes) do if type(id)~='string' then return redis.error_reply('invalid deletion') end end
local ht=redis.call('TYPE',KEYS[1]).ok; local zt=redis.call('TYPE',KEYS[3]).ok
if (ht~='none' and ht~='hash') or (zt~='none' and zt~='zset') then return redis.error_reply('invalid storage type') end
local marker=redis.call('HGET',KEYS[1],'state')
if marker and string.sub(marker,1,8)=='airlock:' and marker~='airlock:v2' then return redis.error_reply('incomplete layout') end
redis.call('HSET',KEYS[1],'state','airlock:v2:incomplete','meta','')
if p.Migrate then
 for _,field in ipairs(redis.call('HKEYS',KEYS[1])) do if string.sub(field,1,2)=='r:' then redis.call('HDEL',KEYS[1],field) end end
 redis.call('DEL',KEYS[3])
end
for _,id in ipairs(p.Deletes) do redis.call('HDEL',KEYS[1],'r:'..id); redis.call('ZREM',KEYS[3],id) end
for id,value in pairs(p.Records) do redis.call('HSET',KEYS[1],'r:'..id,value) end
for id,score in pairs(p.Expires) do redis.call('ZADD',KEYS[3],score,id) end
redis.call('HSET',KEYS[1],'state','airlock:v2','active',ARGV[2],'meta',p.Meta)
return 1
`

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
	if !ok || kind < 0 || kind > 2 {
		return airlock.Result{}, false, airlock.ErrCorruptState
	}
	snapshot, now, err := decodeRead([]any{min(kind, int64(1)), values[1], values[2], values[3]})
	if err != nil {
		return airlock.Result{}, false, err
	}
	state, err := airlock.DecodeState(snapshot)
	if err != nil {
		return airlock.Result{}, false, err
	}
	history := airlock.RetainedHistory{}
	token := ""
	if kind == 2 {
		if state == nil || len(values) != 10 {
			return airlock.Result{}, false, airlock.ErrCorruptState
		}
		fields, fok := values[8].(int64)
		total, tok := values[9].(int64)
		if !fok || !tok || fields != int64(len(state.Records))+total+3 {
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
			if rec == nil {
				return airlock.Result{}, false, airlock.ErrCorruptState
			}
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
	projection, active, err := partitionPublication(next, history, token, publicationBaseline, kind != 2)
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

func partitionPublication(state *airlock.SharedState, history airlock.RetainedHistory, token string, before map[string]recordVersion, migrate bool) ([]byte, []byte, error) {
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
		Migrate bool
	}{string(meta), map[string]string{}, map[string]int64{}, []string{}, migrate}
	active := *state
	active.Records = make(map[string]*airlock.DriverRecord)
	for id, rec := range state.Records {
		if activeRecord(rec) {
			active.Records[id] = rec
		}
		if old, ok := before[id]; !migrate && ok && old == version(rec) {
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

package redis

// readScript pairs the authoritative snapshot with backend time.
const readScript = `
local state = redis.call('HGET', KEYS[1], 'state')
local now = redis.call('TIME')
if state == false then
 if redis.call('EXISTS', KEYS[1]) ~= 0 then return redis.error_reply('missing snapshot') end
 return {0, '', now[1], now[2]}
end
return {1, state, now[1], now[2]}
`

// casScript compares snapshot/time guards, prepares derived request records, then
// publishes the authoritative state and metadata together in its final HSET.
const casScript = `
if #KEYS ~= 1 or (#ARGV ~= 5 and #ARGV ~= 6) then return redis.error_reply('invalid arguments') end
if ARGV[1] ~= '0' and ARGV[1] ~= '1' then return redis.error_reply('invalid existence') end
if #ARGV[3] == 0 then return redis.error_reply('empty snapshot') end
local function integer(s)
 if not string.match(s, '^%d+$') then return nil end
 local n = tonumber(s)
 if not n or n > 9007199254740991 or n < 0 or n ~= math.floor(n) then return nil end
 return n
end
local lower = integer(ARGV[4])
local upper = integer(ARGV[5])
if not lower or not upper or (upper ~= 0 and upper <= lower) then return redis.error_reply('invalid window') end
local old = redis.call('HGET', KEYS[1], 'state')
if ARGV[1] == '0' then
 if old ~= false then return 0 end
 if redis.call('EXISTS', KEYS[1]) ~= 0 then return redis.error_reply('missing snapshot') end
else
 if old == false or old ~= ARGV[2] then return 0 end
end
local tm = redis.call('TIME')
local sec = integer(tm[1])
local us = integer(tm[2])
if not sec or not us or us > 999999 or sec > 9007199254 then return redis.error_reply('invalid clock') end
local now = sec * 1000000 + us
if now > 9007199254740991 then return redis.error_reply('invalid clock') end
if now < lower or (upper ~= 0 and now >= upper) then return 0 end
local index = nil
if #ARGV == 6 then
 index = cjson.decode(ARGV[6])
 if type(index) ~= 'table' or type(index.Meta) ~= 'string' or type(index.Records) ~= 'table' then
  return redis.error_reply('invalid observation index')
 end
 for id, value in pairs(index.Records) do
  if type(id) ~= 'string' or type(value) ~= 'string' then return redis.error_reply('invalid observation record') end
 end
 -- Prepare candidate-tagged derived records before publishing the authoritative
 -- state and header in the final HSET. If preparation fails, mismatched/missing
 -- records force a full-state fallback; they cannot authorize work.
 -- Also collect orphaned indexes from an interrupted preparation.
 for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
  if string.sub(field, 1, 2) == 'r:' and index.Records[string.sub(field, 3)] == nil then
   redis.call('HDEL', KEYS[1], field)
  end
 end
 for id, value in pairs(index.Records) do redis.call('HSET', KEYS[1], 'r:' .. id, value) end
 redis.call('HSET', KEYS[1], 'state', ARGV[3], 'meta', index.Meta)
else
 redis.call('HSET', KEYS[1], 'state', ARGV[3], 'meta', '')
end
return 1
`

// observeScript returns only the resource header, one request and backend time.
// Candidate publication tags are checked in Go; missing/incomplete indexes fall back.
const observeScript = `
local values = redis.call('HMGET', KEYS[1], 'meta', 'r:' .. ARGV[1])
local now = redis.call('TIME')
return {values[1] or '', values[2] or '', now[1], now[2]}
`

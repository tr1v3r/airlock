package redis

// Read pairs GET with backend time. CAS validates everything before its only
// mutation; no potentially failing command follows SET (Lua has no rollback).
const readScript = `
local state = redis.call('GET', KEYS[1])
local now = redis.call('TIME')
if state == false then return {0, '', now[1], now[2]} end
return {1, state, now[1], now[2]}
`
const casScript = `
if #KEYS ~= 1 or #ARGV ~= 5 then return redis.error_reply('invalid arguments') end
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
local old = redis.call('GET', KEYS[1])
if ARGV[1] == '0' then
 if old ~= false then return 0 end
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
redis.call('SET', KEYS[1], ARGV[3])
return 1
`

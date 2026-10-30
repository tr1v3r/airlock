
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
if type(p)~='table' or type(p.Meta)~='string' or type(p.Records)~='table' or type(p.Expires)~='table' or type(p.Deletes)~='table' or ARGV[2]=='' then return redis.error_reply('invalid publication') end
for id,value in pairs(p.Records) do if type(id)~='string' or type(value)~='string' then return redis.error_reply('invalid record') end end
for id,score in pairs(p.Expires) do if type(id)~='string' or type(score)~='number' or score<=0 or score>9007199254740991 or score~=math.floor(score) or not p.Records[id] then return redis.error_reply('invalid expiry') end end
for _,id in ipairs(p.Deletes) do if type(id)~='string' then return redis.error_reply('invalid deletion') end end
local ht=redis.call('TYPE',KEYS[1]).ok; local zt=redis.call('TYPE',KEYS[3]).ok
if (ht~='none' and ht~='hash') or (zt~='none' and zt~='zset') then return redis.error_reply('invalid storage type') end
local current=redis.call('HGET',KEYS[1],'state')
if current == '' then return redis.error_reply('incomplete resource') end
redis.call('HSET',KEYS[1],'state','','meta','')
for _,id in ipairs(p.Deletes) do redis.call('HDEL',KEYS[1],'r:'..id); redis.call('ZREM',KEYS[3],id) end
for id,value in pairs(p.Records) do redis.call('HSET',KEYS[1],'r:'..id,value) end
for id,score in pairs(p.Expires) do redis.call('ZADD',KEYS[3],score,id) end
redis.call('HSET',KEYS[1],'state',ARGV[2],'meta',p.Meta)
return 1

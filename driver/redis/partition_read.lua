
local active = redis.call('HGET', KEYS[1], 'state')
local tm = redis.call('TIME')
if not active then
 if redis.call('EXISTS', KEYS[1]) ~= 0 or redis.call('EXISTS', KEYS[2]) ~= 0 then return redis.error_reply('incomplete resource') end
 return {0, '', tm[1], tm[2], '', {}, 0, ''}
end
local meta = redis.call('HGET',KEYS[1],'meta')
if active == '' or not meta or meta == '' then return redis.error_reply('incomplete resource') end
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
return {1,active,tm[1],tm[2],meta,records,count,before,redis.call('HLEN',KEYS[1]),redis.call('ZCARD',KEYS[2])}

package redis

// commitGuard checks ownership, backend time and common publication fields
// before either full or incremental publication can mutate derived records.
const commitGuard = `
	if #KEYS ~= 2 or #ARGV ~= 5 then return redis.error_reply('invalid arguments') end
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

	local index = cjson.decode(ARGV[5])
	if type(index) ~= 'table' or type(index.Meta) ~= 'string' or type(index.Records) ~= 'table' then
		return redis.error_reply('invalid observation index')
	end
	for id, value in pairs(index.Records) do
		if type(id) ~= 'string' or type(value) ~= 'string' then
			return redis.error_reply('invalid observation record')
		end
	end

`

// commitScript prepares candidate-tagged records, then atomically publishes
// state and metadata. Every updater holds the resource lock.
const commitScript = commitGuard + `
	-- Prepare candidate-tagged derived records before publishing state and the
	-- matching header. Failed preparation cannot authorize work through Observe.
	-- Also collect orphaned indexes from interrupted preparation.
	for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
		if string.sub(field, 1, 2) == 'r:' and index.Records[string.sub(field, 3)] == nil then
			redis.call('HDEL', KEYS[1], field)
		end
	end

	for id, value in pairs(index.Records) do redis.call('HSET', KEYS[1], 'r:' .. id, value) end
	redis.call('HSET', KEYS[1], 'state', ARGV[2], 'meta', index.Meta)
	return 1
`

// observeScript returns only the resource header, one request and backend time.
// Candidate publication tags are checked in Go; missing/incomplete indexes fall back.
const observeScript = `
	local values = redis.call('HMGET', KEYS[1], 'meta', 'r:' .. ARGV[1])
	local now = redis.call('TIME')

	return {values[1] or '', values[2] or '', now[1], now[2]}
`

// acquireUpdateLockScript installs a bounded owner lease without refreshing an
// existing owner's TTL. The integer response distinguishes contention from I/O errors.
const acquireUpdateLockScript = `
	if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then return 1 end
	return 0
`

// releaseUpdateLockScript never deletes a lock belonging to a newer updater.
const releaseUpdateLockScript = `
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('DEL', KEYS[1])
	end
	return 0
`

// deltaCommitScript publishes every changed projection together with state/meta
// in ONE HSET. A stable generation token lets old and new readers reuse unchanged
// projections. At most 256 changed/deleted records keeps unpack below Lua's stack
// limit. Larger transitions use the full candidate-tagged publication protocol.
const deltaCommitScript = commitGuard + `
	if type(index.Deletes) ~= 'table' then return redis.error_reply('invalid deletions') end
	local changes = 0
	local args = {'state', ARGV[2], 'meta', index.Meta}
	for id, value in pairs(index.Records) do
		changes = changes + 1
		args[#args + 1] = 'r:' .. id
		args[#args + 1] = value
	end
	for _, id in pairs(index.Deletes) do
		if type(id) ~= 'string' then return redis.error_reply('invalid deletion') end
		changes = changes + 1
	end
	if changes > 256 then return redis.error_reply('oversized delta') end
	-- Deleting a projection early only makes Observe fall back to the snapshot.
	-- All potentially authorizing record changes are in the final atomic HSET.
	for _, id in pairs(index.Deletes) do redis.call('HDEL', KEYS[1], 'r:' .. id) end
	redis.call('HSET', KEYS[1], unpack(args))
	return 1
`

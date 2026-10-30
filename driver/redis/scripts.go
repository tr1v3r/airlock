package redis

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

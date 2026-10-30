
	local values = redis.call('HMGET', KEYS[1], 'meta', 'r:' .. ARGV[1])
	local now = redis.call('TIME')

	return {values[1] or '', values[2] or '', now[1], now[2]}

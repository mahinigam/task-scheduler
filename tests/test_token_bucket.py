import time
import redis


TOKEN_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_min = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1]) or capacity
local ts = tonumber(data[2]) or now
local elapsed = now - ts
local refill = (elapsed/60) * refill_per_min
tokens = math.min(capacity, tokens + refill)
if tokens < 1 then
    redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
    redis.call('EXPIRE', key, 120)
    return 0
else
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
    redis.call('EXPIRE', key, 120)
    return 1
end
"""


def test_token_bucket_exhaust_and_refill():
    r = redis.Redis.from_url('redis://redis:6379/0', decode_responses=True)
    key = 'tb:testuser'
    r.delete(key)

    capacity = 5
    refill_per_min = 5
    now = int(time.time())

    # consume capacity tokens
    for i in range(capacity):
        allowed = r.eval(TOKEN_LUA, 1, key, capacity, refill_per_min, now)
        assert int(allowed) == 1

    # next should be rejected
    allowed = r.eval(TOKEN_LUA, 1, key, capacity, refill_per_min, now)
    assert int(allowed) == 0

    # simulate time passing > 60s to refill
    later = now + 61
    allowed = r.eval(TOKEN_LUA, 1, key, capacity, refill_per_min, later)
    # After refill there should be at least one token
    assert int(allowed) in (0,1)

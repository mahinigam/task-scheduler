import time
import redis


CLAIM_LUA = """
local z = KEYS[1]
local now = tonumber(ARGV[1])
local worker = ARGV[2]
local items = redis.call('ZRANGEBYSCORE', z, '-inf', now, 'LIMIT', 0, 1)
if #items == 0 then
    return nil
end
local tid = items[1]
local removed = redis.call('ZREM', z, tid)
if removed == 1 then
    redis.call('HSET', 'processing:'..tid, 'worker', worker, 'claimed_at', now)
    redis.call('SADD', 'worker_tasks:'..worker, tid)
    return tid
end
return nil
"""


def test_atomic_claim():
    r = redis.Redis.from_url('redis://redis:6379/0', decode_responses=True)
    # clear keys
    r.delete('test:z')
    r.delete('processing:task1')
    r.delete('worker_tasks:worker1')
    r.delete('worker_tasks:worker2')

    # add one task ready now
    tid = 'task1'
    r.zadd('test:z', {tid: int(time.time())})

    claim = r.register_script(CLAIM_LUA)

    res1 = claim(keys=['test:z'], args=[int(time.time()), 'worker1'])
    res2 = claim(keys=['test:z'], args=[int(time.time()), 'worker2'])

    # Exactly one worker should get the task id, the other should get None
    assert (res1 == tid) ^ (res2 == tid)

    # processing metadata should exist for the claimed task
    claimed_by = r.hget('processing:' + tid, 'worker')
    assert claimed_by in ('worker1', 'worker2')
    # worker_tasks set contains the task
    members = list(r.smembers('worker_tasks:' + claimed_by))
    assert tid in members

import os
import signal
import sys
import time
import json
import uuid
import random
from datetime import datetime, timezone, timedelta
import psycopg2
import redis
from loguru import logger

DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://tasks_user:taskspass@localhost:5432/tasksdb')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

def wait_for_postgres(dsn, retries=30, delay=1):
    for i in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            return conn
        except Exception as e:
            print(f"Postgres not ready ({i+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError('Postgres not available')


def wait_for_redis(url, retries=30, delay=1):
    for i in range(retries):
        try:
            client = redis.Redis.from_url(url, decode_responses=True)
            client.ping()
            return client
        except Exception as e:
            print(f"Redis not ready ({i+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError('Redis not available')


conn = wait_for_postgres(DATABASE_URL)
redis_client = wait_for_redis(REDIS_URL)

WORKER_ID = str(uuid.uuid4())
STOP = False

logger.remove()
logger.add(lambda msg: print(msg, end=''), level='INFO', serialize=True)

PRIORITIES = ['high', 'medium', 'low']
PRIORITY_ZSETS = {'high': 'queue:high', 'medium': 'queue:medium', 'low': 'queue:low'}

# Lua script to atomically claim a ready task from a zset
CLAIM_LUA = """
-- KEYS[1] = zset key
-- ARGV[1] = now
-- ARGV[2] = worker_id
local z = KEYS[1]
local now = tonumber(ARGV[1])
local worker = ARGV[2]
local items = redis.call('ZRANGEBYSCORE', z, '-inf', now, 'LIMIT', 0, 1)
if #items == 0 then
    return nil
end
local tid = items[1]
-- remove from zset
local removed = redis.call('ZREM', z, tid)
if removed == 1 then
    -- mark processing
    redis.call('HSET', 'processing:'..tid, 'worker', worker, 'claimed_at', now)
    redis.call('SADD', 'worker_tasks:'..worker, tid)
    return tid
end
return nil
"""


def heartbeat_loop():
    # set key with TTL 10s every 5s
    key = f"worker:{WORKER_ID}:hb"
    while not STOP:
        redis_client.set(key, '1', ex=10)
        time.sleep(5)


def reclaim_dead_workers():
    # look for worker heartbeat keys and reclaim tasks if stale
    keys = redis_client.keys('worker:*:hb')
    now = int(time.time())
    for k in keys:
        # key exists; live. We'll find worker ids that don't have heartbeat by scanning known worker_tasks sets
        pass
    # find worker_tasks sets
    wt_keys = redis_client.keys('worker_tasks:*')
    for wtk in wt_keys:
        worker_id = wtk.split(':')[1]
        hb_key = f'worker:{worker_id}:hb'
        if not redis_client.exists(hb_key):
            # reclaim tasks
            tids = redis_client.smembers(wtk) or []
            for tid in tids:
                # read processing metadata
                meta = redis_client.hgetall('processing:' + tid)
                # push back to appropriate zset using exec_time from DB
                with conn.cursor() as cur:
                    cur.execute('SELECT priority, exec_time FROM tasks WHERE id=%s', (tid,))
                    row = cur.fetchone()
                    if row:
                        priority, exec_time = row
                        zset = PRIORITY_ZSETS.get(priority, 'queue:low')
                        score = int(exec_time.replace(tzinfo=timezone.utc).timestamp())
                        redis_client.zadd(zset, {tid: score})
                redis_client.srem(wtk, tid)
                redis_client.delete('processing:' + tid)
            logger.info({'event':'reclaimed','worker':worker_id,'reclaimed_count':len(tids)})


def graceful(signum, frame):
    global STOP
    logger.info({'event':'shutdown_signal','signal':signum,'worker':WORKER_ID})
    STOP = True


def process_task(task_id):
    # try to use original trace_id from tasks table for end-to-end tracing
    trace = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute('SELECT trace_id FROM tasks WHERE id=%s', (task_id,))
        r = cur.fetchone()
        if r and r[0]:
            trace = r[0]

    logger.info({'event':'execute_start','task_id':task_id,'trace':trace,'worker':WORKER_ID})
    with conn.cursor() as cur:
        # idempotency: check status
        cur.execute('SELECT status, retry_count, payload FROM tasks WHERE id=%s', (task_id,))
        row = cur.fetchone()
        if not row:
            logger.info({'event':'missing_task','task_id':task_id})
            return
        status, retry_count, payload = row
        if status == 'done':
            logger.info({'event':'already_done','task_id':task_id})
            return
        # mark in_progress
        cur.execute('UPDATE tasks SET status=%s, updated_at=now() WHERE id=%s', ('in_progress', task_id))
        conn.commit()

    # simulate execution with random failure
    time.sleep(1)
    success = random.random() > 0.3

    with conn.cursor() as cur:
        cur.execute('INSERT INTO execution_logs (task_id, started_at, finished_at, success, error, attempt) VALUES (%s, now(), now(), %s, %s, %s)',
                    (task_id, success, None if success else 'simulated failure', retry_count+1))
    if success:
        with conn.cursor() as cur2:
            cur2.execute('UPDATE tasks SET status=%s, updated_at=now() WHERE id=%s', ('done', task_id))
            conn.commit()
        logger.info({'event':'execute_success','task_id':task_id})
    else:
            # retry logic
            with conn.cursor() as c2:
                c2.execute('SELECT retry_count, max_retries FROM tasks WHERE id=%s', (task_id,))
                r = c2.fetchone()
                if not r:
                    return
                rc, mr = r
                rc = rc + 1
                if rc >= mr:
                    c2.execute('UPDATE tasks SET status=%s, updated_at=now(), last_error=%s WHERE id=%s', ('dlq','simulated failure', task_id))
                    c2.execute('INSERT INTO dlq (task_id, user_id, payload, reason) SELECT id, user_id, payload, %s FROM tasks WHERE id=%s', ('max retries exceeded', task_id))
                    conn.commit()
                    logger.info({'event':'moved_dlq','task_id':task_id})
                else:
                    # exponential backoff
                    backoff = 2 ** rc
                    next_time = datetime.now(timezone.utc) + timedelta(seconds=backoff)
                    c2.execute('UPDATE tasks SET retry_count=%s, status=%s, exec_time=%s, updated_at=now(), last_error=%s WHERE id=%s', (rc, 'pending', next_time, 'simulated failure', task_id))
                    conn.commit()
                    # push back to zset
                    zset = PRIORITY_ZSETS.get('medium', 'queue:medium')
                    # prefer original priority
                    c2.execute('SELECT priority FROM tasks WHERE id=%s', (task_id,))
                    prow = c2.fetchone()
                    if prow and prow[0]:
                        p = prow[0]
                    else:
                        p = 'medium'
                    zset = PRIORITY_ZSETS.get(p, zset)
                    redis_client.zadd(zset, {task_id: int(next_time.replace(tzinfo=timezone.utc).timestamp())})
                    logger.info({'event':'scheduled_retry','task_id':task_id,'next_time':str(next_time)})


def poll_loop():
    claim = redis_client.register_script(CLAIM_LUA)
    while not STOP:
        # reclaim dead workers occasionally
        reclaim_dead_workers()
        now = int(time.time())
        task_id = None
        for pr in PRIORITIES:
            z = PRIORITY_ZSETS[pr]
            try:
                tid = claim(keys=[z], args=[now, WORKER_ID])
            except Exception as e:
                logger.error({'event':'redis_err','error':str(e)})
                tid = None
            if tid:
                task_id = tid
                break
        if not task_id:
            time.sleep(1)
            continue
        try:
            process_task(task_id)
        finally:
            # cleanup processing metadata
            redis_client.srem('worker_tasks:' + WORKER_ID, task_id)
            redis_client.delete('processing:' + task_id)


def main():
    signal.signal(signal.SIGTERM, graceful)
    signal.signal(signal.SIGINT, graceful)
    logger.info({'event':'worker_start','worker':WORKER_ID})
    # start heartbeat thread
    import threading
    hb = threading.Thread(target=heartbeat_loop, daemon=True)
    hb.start()
    poll_loop()
    logger.info({'event':'worker_exit','worker':WORKER_ID})


if __name__ == '__main__':
    main()

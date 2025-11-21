import os
import signal
import sys
import time
import json
import uuid
import random
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2 import pool
import redis
from loguru import logger
from prometheus_client import start_http_server, Counter

DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://tasks_user:taskspass@localhost:5432/tasksdb')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')


def wait_for_postgres(dsn, retries=30, delay=1):
    for i in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            return True
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


_ = wait_for_postgres(DATABASE_URL)
redis_client = wait_for_redis(REDIS_URL)

# DB pool configuration
DB_POOL_MIN = int(os.getenv('DB_POOL_MIN', '1'))
DB_POOL_MAX = int(os.getenv('DB_POOL_MAX', '5'))

# create a simple connection pool
pg_pool = pool.SimpleConnectionPool(DB_POOL_MIN, DB_POOL_MAX, DATABASE_URL)

WORKER_ID = str(uuid.uuid4())
STOP = False

logger.remove()
logger.add(lambda msg: print(msg, end=''), level='INFO', serialize=True)

PRIORITIES = ['high', 'medium', 'low']
PRIORITY_ZSETS = {'high': 'queue:high', 'medium': 'queue:medium', 'low': 'queue:low'}

# Prometheus metrics for worker
TASKS_PROCESSED = Counter('worker_tasks_processed_total', 'Total tasks processed by this worker')
TASKS_SUCCESS = Counter('worker_tasks_success_total', 'Total successful tasks')
TASKS_FAILURE = Counter('worker_tasks_failure_total', 'Total failed tasks')
TASKS_RETRIED = Counter('worker_tasks_retried_total', 'Total retried tasks')
TASKS_DLQ = Counter('worker_tasks_dlq_total', 'Total tasks moved to DLQ')

# Lua script to atomically claim the first ready task from a list of zsets
# KEYS = ordered zset keys (high -> medium -> low)
# ARGV[1] = now, ARGV[2] = worker_id
CLAIM_LUA = """
local now = tonumber(ARGV[1])
local worker = ARGV[2]
for i=1,#KEYS do
    local z = KEYS[i]
    local items = redis.call('ZRANGEBYSCORE', z, '-inf', now, 'LIMIT', 0, 1)
    if #items > 0 then
        local tid = items[1]
        local removed = redis.call('ZREM', z, tid)
        if removed == 1 then
            redis.call('HSET', 'processing:'..tid, 'worker', worker, 'claimed_at', now)
            redis.call('SADD', 'worker_tasks:'..worker, tid)
            return tid
        end
    end
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
    wt_keys = redis_client.keys('worker_tasks:*')
    for wtk in wt_keys:
        worker_id = wtk.split(':')[1]
        hb_key = f'worker:{worker_id}:hb'
        if not redis_client.exists(hb_key):
            # reclaim tasks
            tids = redis_client.smembers(wtk) or []
            for tid in list(tids):
                # defensive: ignore non-UUID entries which can appear in Redis
                try:
                    uuid.UUID(tid)
                except Exception:
                    # remove noisy/non-conforming entry and its metadata, continue
                    try:
                        redis_client.srem(wtk, tid)
                        redis_client.delete('processing:' + tid)
                    except Exception:
                        pass
                    continue
                # read processing metadata
                meta = redis_client.hgetall('processing:' + tid)
                # push back to appropriate zset using exec_time from DB
                db_conn = None
                try:
                    db_conn = pg_pool.getconn()
                    with db_conn.cursor() as cur:
                        cur.execute('SELECT priority, exec_time FROM tasks WHERE id=%s', (tid,))
                        row = cur.fetchone()
                        if row:
                            priority, exec_time = row
                            zset = PRIORITY_ZSETS.get(priority, 'queue:low')
                            # preserve same priority ordering used at enqueue time by including a tiny offset
                            try:
                                base_ts = float(exec_time.replace(tzinfo=timezone.utc).timestamp())
                            except Exception:
                                base_ts = time.time()
                            offset = 0.0
                            # map priority to offsets defined in app; mirror the same ordering
                            if priority == 'high':
                                offset = -0.001
                            elif priority == 'low':
                                offset = 0.001
                            score = base_ts + offset
                            redis_client.zadd(zset, {tid: score})
                except Exception as e:
                    # invalid task id format or DB error; skip reclaim for this id but clean its redis metadata
                    logger.error({'event': 'reclaim_error', 'task_id': tid, 'error': str(e)})
                    try:
                        if db_conn:
                            db_conn.rollback()
                    except Exception:
                        pass
                    redis_client.srem(wtk, tid)
                    redis_client.delete('processing:' + tid)
                    continue
                finally:
                    if db_conn:
                        pg_pool.putconn(db_conn)

            redis_client.srem(wtk, *list(tids))
            for tid in tids:
                redis_client.delete('processing:' + tid)
            logger.info({'event': 'reclaimed', 'worker': worker_id, 'reclaimed_count': len(tids)})


def graceful(signum, frame):
    global STOP
    logger.info({'event': 'shutdown_signal', 'signal': signum, 'worker': WORKER_ID})
    STOP = True


def process_task(task_id):
    # try to use original trace_id from tasks table for end-to-end tracing
    trace = str(uuid.uuid4())
    db_conn = None
    try:
        db_conn = pg_pool.getconn()
        try:
            with db_conn.cursor() as cur:
                cur.execute('SELECT trace_id FROM tasks WHERE id=%s', (task_id,))
                r = cur.fetchone()
                if r and r[0]:
                    trace = r[0]
        except Exception as e:
            logger.error({'event': 'db_error_trace', 'task_id': task_id, 'error': str(e)})
            try:
                db_conn.rollback()
            except Exception:
                pass
            # continue with generated trace
    finally:
        if db_conn:
            pg_pool.putconn(db_conn)

    logger.info({'event': 'execute_start', 'task_id': task_id, 'trace': trace, 'worker': WORKER_ID})
    try:
        TASKS_PROCESSED.inc()
    except Exception:
        pass

    # idempotency & mark in_progress
    db_conn = None
    try:
        db_conn = pg_pool.getconn()
        try:
            with db_conn.cursor() as cur:
                cur.execute('SELECT status, retry_count, payload FROM tasks WHERE id=%s', (task_id,))
                row = cur.fetchone()
                if not row:
                    logger.info({'event': 'missing_task', 'task_id': task_id})
                    return
                status, retry_count, payload = row
                if status == 'done':
                    logger.info({'event': 'already_done', 'task_id': task_id})
                    return
                # mark in_progress
                cur.execute('UPDATE tasks SET status=%s, updated_at=now() WHERE id=%s', ('in_progress', task_id))
                db_conn.commit()
        except Exception as e:
            logger.error({'event': 'db_error_idempotency', 'task_id': task_id, 'error': str(e)})
            try:
                db_conn.rollback()
            except Exception:
                pass
            return
    finally:
        if db_conn:
            pg_pool.putconn(db_conn)

    # simulate execution with random failure
    time.sleep(1)
    success = random.random() > 0.3

    # record attempt
    db_conn = None
    try:
        db_conn = pg_pool.getconn()
        try:
            with db_conn.cursor() as cur:
                cur.execute('INSERT INTO execution_logs (task_id, started_at, finished_at, success, error, attempt) VALUES (%s, now(), now(), %s, %s, %s)',
                            (task_id, success, None if success else 'simulated failure', retry_count + 1))
                db_conn.commit()
        except Exception as e:
            logger.error({'event': 'db_error_log', 'task_id': task_id, 'error': str(e)})
            try:
                db_conn.rollback()
            except Exception:
                pass
            return
    finally:
        if db_conn:
            pg_pool.putconn(db_conn)

    if success:
        db_conn = None
        try:
            db_conn = pg_pool.getconn()
            try:
                with db_conn.cursor() as cur2:
                    cur2.execute('UPDATE tasks SET status=%s, updated_at=now() WHERE id=%s', ('done', task_id))
                    db_conn.commit()
            except Exception as e:
                logger.error({'event': 'db_error_mark_done', 'task_id': task_id, 'error': str(e)})
                try:
                    db_conn.rollback()
                except Exception:
                    pass
                return
        finally:
            if db_conn:
                pg_pool.putconn(db_conn)
        logger.info({'event': 'execute_success', 'task_id': task_id})
        try:
            TASKS_SUCCESS.inc()
        except Exception:
            pass
    else:
        # retry logic
        db_conn = None
        try:
            db_conn = pg_pool.getconn()
            try:
                with db_conn.cursor() as c2:
                    c2.execute('SELECT retry_count, max_retries FROM tasks WHERE id=%s', (task_id,))
                    r = c2.fetchone()
                    if not r:
                        return
                    rc, mr = r
                    rc = rc + 1
                    if rc >= mr:
                        c2.execute('UPDATE tasks SET status=%s, updated_at=now(), last_error=%s WHERE id=%s', ('dlq', 'simulated failure', task_id))
                        c2.execute('INSERT INTO dlq (task_id, user_id, payload, reason) SELECT id, user_id, payload, %s FROM tasks WHERE id=%s', ('max retries exceeded', task_id))
                        db_conn.commit()
                        logger.info({'event': 'moved_dlq', 'task_id': task_id})
                        try:
                            TASKS_DLQ.inc()
                        except Exception:
                            pass
                    else:
                        # exponential backoff
                        backoff = 2 ** rc
                        next_time = datetime.now(timezone.utc) + timedelta(seconds=backoff)
                        c2.execute('UPDATE tasks SET retry_count=%s, status=%s, exec_time=%s, updated_at=now(), last_error=%s WHERE id=%s', (rc, 'pending', next_time, 'simulated failure', task_id))
                        db_conn.commit()
                        # push back to zset with a tiny priority offset consistent with enqueue
                        zset = PRIORITY_ZSETS.get('medium', 'queue:medium')
                        # prefer original priority
                        c2.execute('SELECT priority FROM tasks WHERE id=%s', (task_id,))
                        prow = c2.fetchone()
                        if prow and prow[0]:
                            p = prow[0]
                        else:
                            p = 'medium'
                        zset = PRIORITY_ZSETS.get(p, zset)
                        # compute timestamp with priority offset
                        try:
                            base_ts = float(next_time.replace(tzinfo=timezone.utc).timestamp())
                        except Exception:
                            base_ts = time.time()
                        p_offset = 0.0
                        # prefer original priority
                        c2.execute('SELECT priority FROM tasks WHERE id=%s', (task_id,))
                        prow = c2.fetchone()
                        if prow and prow[0]:
                            p = prow[0]
                        else:
                            p = 'medium'
                        if p == 'high':
                            p_offset = -0.001
                        elif p == 'low':
                            p_offset = 0.001
                        score = base_ts + p_offset
                        redis_client.zadd(zset, {task_id: score})
                        logger.info({'event': 'scheduled_retry', 'task_id': task_id, 'next_time': str(next_time)})
                        try:
                            TASKS_RETRIED.inc()
                        except Exception:
                            pass
            except Exception as e:
                logger.error({'event': 'db_error_retry', 'task_id': task_id, 'error': str(e)})
                try:
                    db_conn.rollback()
                except Exception:
                    pass
                return
        finally:
            if db_conn:
                pg_pool.putconn(db_conn)


def poll_loop():
    claim = redis_client.register_script(CLAIM_LUA)
    # precompute ordered zset keys for the script
    ordered_zsets = [PRIORITY_ZSETS[p] for p in PRIORITIES]
    while not STOP:
        # reclaim dead workers occasionally
        reclaim_dead_workers()
        now = float(time.time())
        task_id = None
        try:
            tid = claim(keys=ordered_zsets, args=[now, WORKER_ID])
        except Exception as e:
            logger.error({'event': 'redis_err', 'error': str(e)})
            tid = None
        if not tid:
            time.sleep(1)
            continue
        task_id = tid
        try:
            process_task(task_id)
        finally:
            # cleanup processing metadata
            redis_client.srem('worker_tasks:' + WORKER_ID, task_id)
            redis_client.delete('processing:' + task_id)


def main():
    signal.signal(signal.SIGTERM, graceful)
    signal.signal(signal.SIGINT, graceful)
    logger.info({'event': 'worker_start', 'worker': WORKER_ID})
    # start prometheus metrics server for this worker (port configurable)
    try:
        mp = int(os.getenv('WORKER_METRICS_PORT', '8001'))
        start_http_server(mp)
        logger.info({'event': 'metrics_server_started', 'port': mp})
    except Exception:
        logger.error({'event': 'metrics_start_failed'})
    # start heartbeat thread
    import threading
    hb = threading.Thread(target=heartbeat_loop, daemon=True)
    hb.start()
    poll_loop()
    logger.info({'event': 'worker_exit', 'worker': WORKER_ID})


if __name__ == '__main__':
    main()

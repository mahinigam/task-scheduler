import os
import uuid
import time
import json
from datetime import datetime, timezone
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
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

app = FastAPI()

logger.remove()
logger.add(lambda msg: print(msg, end=''), level='INFO', serialize=True)

PRIORITY_ZSETS = {'high': 'queue:high', 'medium': 'queue:medium', 'low': 'queue:low'}


class TaskIn(BaseModel):
    user_id: str
    payload: dict
    exec_time: datetime
    priority: str


def token_bucket_allow(user_id: str, capacity=50, refill_per_min=50):
    # simple token bucket in Redis using Lua to be atomic
    key = f"tb:{user_id}"
    now = int(time.time())
    # tokens: current tokens, ts: last refill timestamp
    script = """
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
    allowed = redis_client.eval(script, 1, key, capacity, refill_per_min, now)
    return bool(allowed)


@app.post('/tasks')
def submit_task(task: TaskIn, request: Request):
    trace_id = request.headers.get('X-Trace-Id', str(uuid.uuid4()))
    logger.bind(trace_id=trace_id).info('submit_request', extra={'user': task.user_id})

    if task.priority.lower() not in PRIORITY_ZSETS:
        raise HTTPException(status_code=400, detail='invalid priority')

    if not token_bucket_allow(task.user_id):
        raise HTTPException(status_code=429, detail='rate limit exceeded')

    task_id = str(uuid.uuid4())
    payload_json = json.dumps(task.payload)

    # insert into Postgres (persist trace_id for end-to-end tracing)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO tasks (id, user_id, payload, priority, exec_time, status, retry_count, trace_id) VALUES (%s,%s,%s,%s,%s,'pending',0,%s)",
            (task_id, task.user_id, payload_json, task.priority.lower(), task.exec_time, trace_id)
        )
        conn.commit()

    # push to Redis priority zset with score=exec_time epoch
    score = int(task.exec_time.replace(tzinfo=timezone.utc).timestamp())
    zset = PRIORITY_ZSETS[task.priority.lower()]
    redis_client.zadd(zset, {task_id: score})

    logger.bind(trace_id=trace_id).info('task_created', extra={'task_id': task_id, 'priority': task.priority})
    return {'task_id': task_id, 'trace_id': trace_id}

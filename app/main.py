import logging
import json
import time
import uuid
import os
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import JSONResponse
from redis import asyncio as aioredis
from sqlalchemy import text
from contextlib import asynccontextmanager

from app.models import TaskCreate, TaskResponse, Priority, TaskStatus
from app.database import get_db, engine

# --- Configuration ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RATE_LIMIT_MAX_REQUESTS = 50
RATE_LIMIT_WINDOW_SECONDS = 60

# --- Logging Setup ---
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "trace_id": getattr(record, "trace_id", None),
        }
        return json.dumps(log_record)

logger = logging.getLogger("api")
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# --- Redis Setup ---
redis_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield
    await redis_client.close()

app = FastAPI(lifespan=lifespan)

# --- Middleware ---
@app.middleware("http")
async def structured_logging_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-ID", str(uuid.uuid4()))
    request.state.trace_id = trace_id
    
    # Inject trace_id into logger context (using a filter or adapter would be cleaner, 
    # but for simplicity we'll manually pass it or rely on context vars if we had them.
    # Here we'll just log the request with the trace_id)
    
    logger.info(f"Request started: {request.method} {request.url}", extra={"trace_id": trace_id})
    
    start_time = time.time()
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        logger.info(f"Request completed: {response.status_code} in {process_time:.4f}s", extra={"trace_id": trace_id})
        response.headers["X-Trace-ID"] = trace_id
        return response
    except Exception as e:
        logger.error(f"Request failed: {str(e)}", extra={"trace_id": trace_id})
        raise

# --- Rate Limiting ---
# --- Rate Limiting ---
TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local info = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(info[1])
local last_updated = tonumber(info[2])

if not tokens then
    tokens = capacity
    last_updated = now
end

local delta = math.max(0, now - last_updated)
local filled_tokens = math.min(capacity, tokens + (delta * refill_rate))

if filled_tokens >= requested then
    local new_tokens = filled_tokens - requested
    redis.call('HMSET', key, 'tokens', new_tokens, 'last_updated', now)
    redis.call('EXPIRE', key, 60)
    return 1
else
    return 0
end
"""

async def check_rate_limit(request: Request):
    # Token Bucket Implementation
    # 50 tasks / minute = 50 capacity, refill rate ~0.833 tokens/sec
    user_id = request.client.host
    key = f"rate_limit:{user_id}"
    
    capacity = 50
    refill_rate = 50 / 60.0
    now = time.time()
    requested = 1
    
    allowed = await redis_client.eval(
        TOKEN_BUCKET_SCRIPT,
        1,
        key,
        capacity,
        refill_rate,
        now,
        requested
    )
    
    if not allowed:
        raise HTTPException(status_code=429, detail="Too Many Requests")

# --- Endpoints ---

@app.post("/tasks", response_model=TaskResponse, dependencies=[Depends(check_rate_limit)])
async def submit_task(task: TaskCreate, request: Request):
    trace_id = request.state.trace_id
    task_id = uuid.uuid4()
    
    # Priority Mapping: High=1, Medium=2, Low=3 (Lower score = Higher priority in ZPOPMIN)
    priority_score = {
        Priority.HIGH: 1,
        Priority.MEDIUM: 2,
        Priority.LOW: 3
    }[task.priority]
    
    # 1. Persist to Postgres
    # We use raw SQL or SQLAlchemy core for async simplicity with the setup we have
    async with engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO tasks (id, payload, priority, status, execution_time)
                VALUES (:id, :payload, :priority, :status, :execution_time)
            """),
            {
                "id": task_id,
                "payload": json.dumps(task.payload),
                "priority": task.priority.value,
                "status": TaskStatus.PENDING.value,
                "execution_time": task.execution_time
            }
        )
    
    # 2. Push to Redis ZSET
    # Key: queue:tasks, Score: priority_score, Member: task_id
    await redis_client.zadd("queue:tasks", {str(task_id): priority_score})
    
    logger.info(f"Task submitted: {task_id}", extra={"trace_id": trace_id})
    
    return TaskResponse(
        id=task_id,
        payload=task.payload,
        priority=task.priority,
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow() # Note: In real app, fetch from DB or use timezone aware
    )



import json
import logging
import asyncio
import time
import random
from redis import asyncio as aioredis
from sqlalchemy import text
from app.database import engine
from app.models import TaskStatus

logger = logging.getLogger("worker")

# Lua script to atomically pop the highest priority task (lowest score)
# and move it to a processing hash.
# KEYS[1] = queue:tasks (ZSET)
# KEYS[2] = task:processing (HASH) - maps task_id to worker_id/timestamp
# ARGV[1] = worker_id
FETCH_TASK_SCRIPT = """
local tasks = redis.call('zrange', KEYS[1], 0, 0)
if #tasks > 0 then
    local task_id = tasks[1]
    redis.call('zrem', KEYS[1], task_id)
    redis.call('hset', KEYS[2], task_id, ARGV[1])
    return task_id
else
    return nil
end
"""

class TaskProcessor:
    def __init__(self, worker_id: str, redis_url: str):
        self.worker_id = worker_id
        self.redis_url = redis_url
        self.redis = None
        self.running = False
        self.running = False
        self.current_task = None
        self.last_reclaim_time = 0

    async def start(self):
        self.redis = aioredis.from_url(self.redis_url, decode_responses=True)
        self.running = True
        logger.info(f"Worker {self.worker_id} started.")
        
        while self.running:
            try:
                task_id = await self.fetch_task()
                if task_id:
                    self.current_task = task_id
                    await self.process_task(task_id)
                    self.current_task = None
                else:
                    # No tasks, sleep briefly
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(1)
            
            # Periodic reclamation (every 10 seconds)
            now = time.time()
            if now - self.last_reclaim_time > 10:
                await self.reclaim_dead_tasks()
                self.last_reclaim_time = now

    async def stop(self):
        logger.info(f"Worker {self.worker_id} stopping...")
        self.running = False
        if self.redis:
            await self.redis.close()

    async def fetch_task(self):
        # Execute Lua script
        task_id = await self.redis.eval(
            FETCH_TASK_SCRIPT, 
            2, 
            "queue:tasks", 
            "task:processing", 
            self.worker_id
        )
        return task_id

    async def process_task(self, task_id: str):
        logger.info(f"Processing task {task_id}")
        
        # 1. Fetch task details from Postgres
        task_data = None
        async with engine.begin() as conn:
            result = await conn.execute(
                text("SELECT payload, priority, execution_time FROM tasks WHERE id = :id"),
                {"id": task_id}
            )
            task_data = result.fetchone()
            
            # Update status to PROCESSING
            await conn.execute(
                text("UPDATE tasks SET status = :status, updated_at = NOW() WHERE id = :id"),
                {"status": TaskStatus.PROCESSING.value, "id": task_id}
            )
            
            # Log start
            await conn.execute(
                text("INSERT INTO execution_logs (task_id, worker_id, status, message) VALUES (:tid, :wid, :stat, :msg)"),
                {"tid": task_id, "wid": self.worker_id, "stat": "STARTED", "msg": "Task started"}
            )

        if not task_data:
            logger.error(f"Task {task_id} not found in DB!")
            await self.cleanup_task(task_id)
            return

        payload = task_data[0] # payload is JSONB
        execution_time = task_data[2] if task_data[2] is not None else 1.0
        
        # 2. Execute Task (Simulate work & failure)
        success = False
        retries = 0
        max_retries = 3
        
        while retries <= max_retries:
            try:
                # Simulate execution time
                await asyncio.sleep(execution_time)
                
                # Simulate random failure (20% chance)
                if random.random() < 0.2:
                    raise Exception("Random simulated failure")
                
                success = True
                break
            except Exception as e:
                retries += 1
                logger.warning(f"Task {task_id} failed attempt {retries}/{max_retries + 1}: {e}")
                
                if retries <= max_retries:
                    # Exponential Backoff: 2s, 4s, 8s
                    wait_time = 2 ** retries
                    logger.info(f"Retrying task {task_id} in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Task {task_id} failed permanently after {retries} retries.")
        
        # 3. Finalize
        async with engine.begin() as conn:
            if success:
                await conn.execute(
                    text("UPDATE tasks SET status = :status, updated_at = NOW() WHERE id = :id"),
                    {"status": TaskStatus.COMPLETED.value, "id": task_id}
                )
                await conn.execute(
                    text("INSERT INTO execution_logs (task_id, worker_id, status, message) VALUES (:tid, :wid, :stat, :msg)"),
                    {"tid": task_id, "wid": self.worker_id, "stat": "COMPLETED", "msg": "Task completed successfully"}
                )
            else:
                await conn.execute(
                    text("UPDATE tasks SET status = :status, updated_at = NOW() WHERE id = :id"),
                    {"status": TaskStatus.FAILED.value, "id": task_id}
                )
                await conn.execute(
                    text("INSERT INTO execution_logs (task_id, worker_id, status, message) VALUES (:tid, :wid, :stat, :msg)"),
                    {"tid": task_id, "wid": self.worker_id, "stat": "FAILED", "msg": "Task failed after retries"}
                )
                # Move to DLQ
                await conn.execute(
                    text("INSERT INTO dlq (task_id, payload, failure_reason) VALUES (:tid, :pay, :reason)"),
                    {"tid": task_id, "pay": json.dumps(payload) if isinstance(payload, dict) else payload, "reason": "Max retries exceeded"}
                )

        # 4. Cleanup Redis
        await self.cleanup_task(task_id)

    async def cleanup_task(self, task_id: str):
        # Remove from processing hash
        await self.redis.hdel("task:processing", task_id)

    async def reclaim_dead_tasks(self):
        """
        Check for tasks claimed by dead workers and re-queue them.
        """
        try:
            # Get all tasks currently being processed
            # task:processing is a Hash: task_id -> worker_id
            processing_tasks = await self.redis.hgetall("task:processing")
            
            if not processing_tasks:
                return

            for task_id, worker_id in processing_tasks.items():
                # Check if worker is alive
                is_alive = await self.redis.exists(f"worker:heartbeat:{worker_id}")
                
                if not is_alive:
                    logger.warning(f"Worker {worker_id} appears dead. Reclaiming task {task_id}...")
                    
                    # Fetch priority to re-queue correctly
                    priority_score = 2 # Default to Medium
                    async with engine.begin() as conn:
                        result = await conn.execute(
                            text("SELECT priority FROM tasks WHERE id = :id"),
                            {"id": task_id}
                        )
                        row = result.fetchone()
                        if row:
                            # Map priority string/enum to score
                            # High=1, Medium=2, Low=3
                            p_str = row[0]
                            if p_str == "High":
                                priority_score = 1
                            elif p_str == "Low":
                                priority_score = 3
                            else:
                                priority_score = 2
                    
                    # Re-queue task
                    await self.redis.zadd("queue:tasks", {task_id: priority_score})
                    
                    # Remove from processing map
                    await self.redis.hdel("task:processing", task_id)
                    
                    logger.info(f"Task {task_id} reclaimed and re-queued.")
                    
        except Exception as e:
            logger.error(f"Error during task reclamation: {e}")

import asyncio
import os
import signal
import uuid
import logging
import json
from worker.processor import TaskProcessor

# Logging Setup (Duplicate of API for now, could be shared)
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
        }
        return json.dumps(log_record)

logger = logging.getLogger("worker")
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

async def heartbeat(redis_url, worker_id):
    """Periodically update worker heartbeat in Redis."""
    from redis import asyncio as aioredis
    redis = aioredis.from_url(redis_url)
    logger.info("Heartbeat started.")
    try:
        while True:
            await redis.setex(f"worker:heartbeat:{worker_id}", 10, "alive")
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        logger.info("Heartbeat stopping.")
    finally:
        await redis.close()

async def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    worker_id = os.getenv("WORKER_ID_PREFIX", "worker") + "-" + str(uuid.uuid4())[:8]
    
    processor = TaskProcessor(worker_id, redis_url)
    
    # Graceful Shutdown Handling
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_signal():
        logger.info("Signal received, shutting down gracefully...")
        stop_event.set()

    loop.add_signal_handler(signal.SIGTERM, handle_signal)
    loop.add_signal_handler(signal.SIGINT, handle_signal)

    # Start Heartbeat
    heartbeat_task = asyncio.create_task(heartbeat(redis_url, worker_id))
    
    # Start Processor
    processor_task = asyncio.create_task(processor.start())

    # Wait for stop signal
    await stop_event.wait()

    # Shutdown sequence
    await processor.stop()
    await processor_task # Wait for processor to finish current task
    
    heartbeat_task.cancel()
    try:
        await heartbeat_task
    except asyncio.CancelledError:
        pass
    
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())

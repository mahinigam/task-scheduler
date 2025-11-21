# Distributed Task Scheduler (prototype)

This repository contains a production-grade design and prototype implementation of a distributed task scheduler using Python (FastAPI), PostgreSQL, and Redis.

Services:
- api: FastAPI service that accepts task submissions. It enforces per-user rate limiting (token bucket) and pushes tasks into Redis priority queues and persists them in Postgres.
- worker: Worker process that claims tasks atomically, executes them (simulated), performs retries with exponential backoff and moves tasks to DLQ after max retries. Workers heartbeat and reclaimer re-enqueues tasks of dead workers.
- postgres: Stores tasks, execution logs, and DLQ
- redis: Priority queues and worker coordination

See `db/schema.sql` for the database schema.

Run locally:

1. Build and start all services

```bash
docker compose up --build
```

2. Submit a task

POST to http://localhost:8000/tasks with JSON body:

{
  "user_id": "user-123",
  "payload": {"action":"do_work"},
  "exec_time": "2025-11-21T12:00:00Z",
  "priority": "high"
}

Notes:
- The prototype focuses on demonstrating the requested features: priority queueing, idempotency, retries, DLQ, rate limiting, graceful shutdown, structured JSON logging, and heartbeat/reclaim.

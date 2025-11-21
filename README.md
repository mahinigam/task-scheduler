# Distributed Task Scheduler

A production-grade distributed task scheduler built with **FastAPI**, **Redis**, and **PostgreSQL**. This system is designed to handle high-concurrency task scheduling with priority queues, fault tolerance, and horizontal scalability.

## Features

### Core Functionality
- **Task Submission API**: REST endpoint to submit tasks with execution estimates and priority levels.
- **Priority Queueing**: Uses Redis Sorted Sets (ZSET) to ensure 'High' priority tasks are processed before 'Low' priority ones.
- **Asynchronous Workers**: Scalable worker pool that pulls jobs from Redis.
- **Fault Tolerance**:
  - **Retries**: Exponential backoff (2s, 4s, 8s) for failed tasks.
  - **Dead Letter Queue (DLQ)**: Tasks exceeding max retries are moved to a DLQ in Postgres for inspection.
- **Idempotency**: Prevents duplicate processing of the same task.

### Advanced Features
- **Token Bucket Rate Limiting**: Lua script-based rate limiter enforcing 50 tasks/minute per IP with smooth refilling.
- **Graceful Shutdown**: Workers finish their current task before shutting down on SIGTERM.
- **Heartbeat & Reclamation**: Workers periodically ping Redis. If a worker dies, a reclaimer process detects the missing heartbeat and re-queues its in-flight tasks.
- **Structured Logging**: JSON logs with Trace IDs for end-to-end request tracking.

## Tech Stack

- **Language**: Python 3.11
- **API Framework**: FastAPI
- **Database**: PostgreSQL (Persistence, History, DLQ)
- **Broker/Queue**: Redis (Priority Queues, Rate Limiting, Coordination)
- **Containerization**: Docker & Docker Compose

## Getting Started

### Prerequisites
- Docker & Docker Compose

### Running the System

1. **Clone the repository**
2. **Start the stack**:
   ```bash
   docker compose up --build
   ```
   This will start:
   - `api`: Available at http://localhost:8000
   - `worker`: Two worker replicas processing tasks
   - `postgres`: Database on port 5432
   - `redis`: Cache/Queue on port 6379

### Verifying the Deployment

A verification script is included to demonstrate priority queueing and rate limiting.

1. Ensure the stack is running.
2. Run the script (requires `requests`):
   ```bash
   pip install requests
   python verification_script.py
   ```
   This script will:
   - Submit low priority tasks followed by high priority tasks (verifying High is processed first).
   - Spam the API with requests to trigger and verify the Rate Limiter.

## API Usage

### Submit a Task

**Endpoint**: `POST /tasks`

**Payload**:
```json
{
  "payload": {
    "action": "send_email",
    "recipient": "user@example.com"
  },
  "priority": "High",
  "execution_time": 1.5
}
```

- `priority`: "High", "Medium", or "Low"
- `execution_time`: Estimated time in seconds (used to simulate work duration)
- `payload`: Arbitrary JSON data

**Response**:
```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "payload": {...},
  "priority": "High",
  "status": "PENDING",
  "created_at": "2025-11-21T10:00:00.000000"
}
```

## Project Structure

```
.
├── app/
│   ├── main.py          # FastAPI entrypoint, Rate Limiter, API Endpoints
│   ├── models.py        # Pydantic models
│   └── database.py      # Database connection logic
├── worker/
│   ├── main.py          # Worker entrypoint, Heartbeat, Signal Handling
│   └── processor.py     # Task processing logic, Retry, Reclaimer
├── docker-compose.yml   # Service orchestration
├── schema.sql           # Database schema
└── verification_script.py # E2E test script
```

## Design Decisions

- **Redis ZSET for Priority**: We use the priority score (High=1, Medium=2, Low=3) as the ZSET score. `ZPOPMIN` (via Lua) ensures the highest priority (lowest score) is always fetched first.
- **Lua for Atomicity**: Both the Rate Limiter and the Task Fetching logic use Lua scripts to ensure operations are atomic and race-condition free.
- **Worker Reclamation**: Workers maintain a "processing" state in Redis. If a worker's heartbeat stops, another worker (or the same one upon restart) detects the stale lock and re-queues the task, ensuring zero data loss during crashes.

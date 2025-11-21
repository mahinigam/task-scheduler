# Distributed Task Scheduler

A production-grade distributed task scheduler built with **FastAPI**, **Redis**, and **PostgreSQL**. This system demonstrates enterprise-level backend architecture with priority queuing, fault tolerance, and horizontal scalability.

## Features

### Core Functionality
- **Task Submission API**: REST endpoint to submit tasks with configurable execution time and priority levels
- **Priority Queueing**: Redis Sorted Sets (ZSET) ensure High priority tasks are processed before Low priority tasks, regardless of submission order
- **Asynchronous Workers**: Horizontally scalable worker pool that claims and processes jobs from Redis
- **Fault Tolerance**:
  - **Automatic Retries**: Exponential backoff (2s → 4s → 8s) for failed tasks
  - **Dead Letter Queue (DLQ)**: Tasks exceeding max retries are persisted in PostgreSQL for manual inspection
- **Idempotency**: Atomic Lua scripts prevent duplicate processing even under race conditions

### Advanced Features
- **Token Bucket Rate Limiting**: Lua script-based rate limiter enforcing 50 tasks/minute per IP with smooth token refilling
- **Graceful Shutdown**: Workers finish their current task before terminating on SIGTERM/SIGINT signals
- **Heartbeat & Task Reclamation**: Workers periodically ping Redis; if a worker crashes, its in-flight tasks are automatically reclaimed and re-queued
- **Structured Logging**: JSON-formatted logs with Trace IDs for end-to-end request tracking across API and workers

## Tech Stack

- **Language**: Python 3.11
- **API Framework**: FastAPI
- **Database**: PostgreSQL (Task persistence, execution history, DLQ)
- **Queue/Broker**: Redis (Priority queues, rate limiting, worker coordination)
- **Containerization**: Docker & Docker Compose

## Local Development

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (optional, for local development without Docker)

### Quick Start

1. **Clone the repository**:
   ```bash
   git clone https://github.com/mahinigam/task-scheduler.git
   cd task-scheduler
   ```

2. **Create a `.env` file** (for local development):
   ```bash
   cat > .env << EOF
   DATABASE_URL=postgresql+asyncpg://postgres:password@postgres:5432/scheduler
   REDIS_URL=redis://redis:6379/0
   EOF
   ```

3. **Start the stack**:
   ```bash
   docker compose up --build
   ```

4. **Access the API**:
   - API Documentation: http://localhost:8000/docs
   - Submit tasks via Swagger UI or curl

### Running Tests

Run the verification script to test priority queueing and rate limiting:

```bash
python verification_script.py
```

This will:
- Submit 5 Low priority tasks followed by 2 High priority tasks
- Verify High priority tasks are processed first
- Test rate limiting by sending 60 requests (expecting ~50 success, ~10 blocked)

## AWS Deployment Guide

Deploy this to production on AWS using managed services (RDS, ElastiCache) or a cost-optimized setup with containerized Redis.

### Architecture Options

**Option A: Fully Managed (Production)**
- EC2 (t2.micro): API + Workers
- RDS PostgreSQL (db.t3.micro): Database
- ElastiCache Redis (cache.t2.micro): Queue

**Option B: Cost-Optimized (Free Tier)**
- EC2 (t2.micro): API + Workers + Redis (Dockerized)
- RDS PostgreSQL (db.t3.micro): Database

This guide uses **Option B** to stay within AWS Free Tier limits.

### Step 1: Create Security Group

1. Go to **EC2** → **Security Groups** → **Create security group**
2. **Name**: `scheduler-sg`
3. **Description**: `Security group for Task Scheduler`
4. **Inbound rules**:
   - SSH: TCP `22`, Source: `My IP` (for server access)
   - API: TCP `8000`, Source: `Anywhere-IPv4 (0.0.0.0/0)`
   - PostgreSQL: TCP `5432`, Source: `scheduler-sg` (allow self-referencing)
   - Redis: TCP `6379`, Source: `scheduler-sg` (allow self-referencing)

### Step 2: Create RDS Database

1. Go to **RDS** → **Create database**
2. **Engine**: PostgreSQL 16.x
3. **Templates**: **Free tier**
4. **Settings**:
   - DB instance identifier: `scheduler-db`
   - Master username: `postgres`
   - Master password: (create a strong password)
5. **Instance configuration**: `db.t3.micro`
6. **Connectivity**:
   - Public access: **No**
   - VPC security group: `scheduler-sg`
7. **Additional configuration**:
   - Initial database name: `scheduler` (Important!)
8. Click **Create database**
9. **Save the endpoint** (e.g., `scheduler-db.xyz.us-east-1.rds.amazonaws.com`)

### Step 3: Launch EC2 Instance

1. Go to **EC2** → **Launch instance**
2. **Name**: `scheduler-server`
3. **OS**: Ubuntu Server 22.04 LTS
4. **Instance type**: `t2.micro` (Free tier eligible)
5. **Key pair**: Create new key pair (`scheduler-key.pem`) and download it
6. **Network settings**: Select existing security group `scheduler-sg`
7. **Storage**: 8 GB gp3 (default)
8. Click **Launch instance**

### Step 4: Deploy the Application

**1. Connect to EC2:**
```bash
chmod 400 scheduler-key.pem
ssh -i "scheduler-key.pem" ubuntu@<EC2_PUBLIC_IP>
```

**2. Install Docker:**
```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose git postgresql-client
sudo usermod -aG docker ubuntu
# Log out and log back in for group change to take effect
exit
ssh -i "scheduler-key.pem" ubuntu@<EC2_PUBLIC_IP>
```

**3. Clone and configure:**
```bash
git clone https://github.com/mahinigam/task-scheduler.git
cd task-scheduler

# Create .env file with AWS RDS endpoint
cat > .env << EOF
DATABASE_URL=postgresql+asyncpg://postgres:YOUR_PASSWORD@YOUR_RDS_ENDPOINT:5432/scheduler
REDIS_URL=redis://redis:6379/0
EOF
```

**4. Initialize database schema:**
```bash
psql "postgresql://postgres:YOUR_PASSWORD@YOUR_RDS_ENDPOINT:5432/scheduler" -f schema.sql
```

**5. Start the application:**
```bash
docker-compose up -d --build
```

**6. Verify deployment:**

Open your browser and visit: `http://<EC2_PUBLIC_IP>:8000/docs`

### Testing Your Deployment

Submit a test task via Swagger UI:

```json
{
  "payload": {
    "action": "test_task",
    "message": "Hello from AWS!"
  },
  "priority": "High",
  "execution_time": 2.0
}
```

Check worker logs on EC2:
```bash
docker-compose logs -f worker
```

### AWS Cost Management

**Free Tier Limits** (12 months from signup):
- **EC2**: 750 hours/month of t2.micro
- **RDS**: 750 hours/month of db.t3.micro + 20GB storage
- **ElastiCache**: 750 hours/month of cache.t2.micro (if used)

**To minimize costs:**

1. **Stop EC2 when not in use**:
   ```bash
   # From AWS Console: EC2 → Instances → Instance State → Stop
   ```

2. **Stop RDS temporarily** (auto-restarts after 7 days):
   ```bash
   # From AWS Console: RDS → Actions → Stop temporarily
   ```

3. **Delete ElastiCache** (cannot be stopped, only deleted):
   ```bash
   # From AWS Console: ElastiCache → Delete
   ```

**Note**: This deployment uses containerized Redis to avoid ElastiCache costs.

## API Usage

### Submit a Task

**Endpoint**: `POST /tasks`

**Request**:
```json
{
  "payload": {
    "action": "send_email",
    "recipient": "user@example.com",
    "subject": "Welcome"
  },
  "priority": "High",
  "execution_time": 1.5
}
```

**Parameters**:
- `priority`: `"High"`, `"Medium"`, or `"Low"` (default: `"Medium"`)
- `execution_time`: Estimated execution time in seconds (used to simulate work duration)
- `payload`: Arbitrary JSON data

**Response** (200 OK):
```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "payload": {...},
  "priority": "High",
  "status": "PENDING",
  "created_at": "2025-11-21T10:00:00.000000"
}
```

**Rate Limiting**:
- The API enforces 50 tasks/minute per IP using a Token Bucket algorithm
- Exceeding this limit returns `429 Too Many Requests`

## Project Structure

```
.
├── app/
│   ├── main.py          # FastAPI app, rate limiter, API endpoints
│   ├── models.py        # Pydantic models (Priority, TaskStatus, etc.)
│   └── database.py      # SQLAlchemy async engine configuration
├── worker/
│   ├── main.py          # Worker entrypoint, heartbeat, signal handlers
│   └── processor.py     # Task processing logic, retry, reclamation
├── docker-compose.yml   # Service orchestration
├── Dockerfile.api       # API container definition
├── Dockerfile.worker    # Worker container definition
├── schema.sql           # Database schema (tasks, execution_logs, dlq)
├── requirements.txt     # Python dependencies
└── verification_script.py # E2E test script
```

## Design Decisions

### Redis ZSET for Priority Queuing
Tasks are stored in a Redis Sorted Set with priority as the score (High=1, Medium=2, Low=3). The atomic Lua script ensures `ZPOPMIN` always fetches the highest priority (lowest score) task first, preventing race conditions.

### Lua Scripts for Atomicity
Both the **Rate Limiter** (Token Bucket refresh) and **Task Fetching** (claim-and-dequeue) use Lua scripts to guarantee atomicity. This eliminates race conditions where multiple workers could claim the same task or a user could bypass rate limits.

### Heartbeat-Based Task Reclamation
Workers periodically update a heartbeat key in Redis (`worker:heartbeat:{worker_id}`) with a 10-second TTL. If a worker crashes, its heartbeat expires. A reclaimer process (running in each worker) detects missing heartbeats and re-queues orphaned tasks, ensuring zero data loss during failures.

### Database Schema
- **tasks**: Primary task store with priority, status, retry count
- **execution_logs**: Audit trail of all task execution attempts
- **dlq** (Dead Letter Queue): Failed tasks for manual investigation

## Future Enhancements

- **Kubernetes Deployment**: Migrate to EKS/GKE for auto-scaling and self-healing
- **Observability**: Integrate Prometheus metrics and Grafana dashboards
- **Admin UI**: Web interface for monitoring queues, retrying DLQ tasks, and viewing logs
- **Task Scheduling**: Support cron-like scheduled tasks
- **Webhooks**: Notify external services upon task completion/failure

## License

MIT License - See LICENSE file for details

## Author

Built by **Mahi Nigam** as a demonstration of production-grade distributed systems architecture.

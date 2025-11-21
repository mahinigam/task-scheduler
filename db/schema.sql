-- Database schema for Task Scheduler
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    trace_id TEXT,
    priority TEXT NOT NULL CHECK (priority IN ('high','medium','low')),
    exec_time TIMESTAMP WITH TIME ZONE NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','in_progress','done','failed','dlq')) DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_exec_time ON tasks (status, exec_time);
CREATE INDEX IF NOT EXISTS idx_tasks_user ON tasks (user_id);

CREATE TABLE IF NOT EXISTS execution_logs (
    id SERIAL PRIMARY KEY,
    task_id UUID REFERENCES tasks(id),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    finished_at TIMESTAMP WITH TIME ZONE,
    success BOOLEAN,
    error TEXT,
    attempt INT
);

CREATE TABLE IF NOT EXISTS dlq (
    id SERIAL PRIMARY KEY,
    task_id UUID,
    user_id TEXT,
    payload JSONB,
    failed_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    reason TEXT
);

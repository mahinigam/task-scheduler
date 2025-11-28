CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    payload JSONB NOT NULL,
    priority VARCHAR(10) NOT NULL, -- 'High', 'Medium', 'Low'
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'
    execution_time FLOAT DEFAULT 1.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS execution_logs (
    id SERIAL PRIMARY KEY,
    task_id UUID REFERENCES tasks(id),
    worker_id VARCHAR(50),
    status VARCHAR(20),
    message TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dlq (
    id SERIAL PRIMARY KEY,
    task_id UUID REFERENCES tasks(id),
    payload JSONB,
    failure_reason TEXT,
    failed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_priority ON tasks(priority);

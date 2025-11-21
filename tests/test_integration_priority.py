import time
import requests
import psycopg2
from datetime import datetime, timezone


API_URL = 'http://localhost:8000'
DB_DSN = 'postgres://tasks_user:taskspass@postgres:5432/tasksdb'


def wait_for_task_execution(conn, task_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with conn.cursor() as cur:
            cur.execute('SELECT started_at FROM execution_logs WHERE task_id=%s ORDER BY id LIMIT 1', (task_id,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
        time.sleep(0.5)
    raise AssertionError(f'task {task_id} did not execute in time')


def test_high_priority_precedes_low():
    # create a low-priority task first
    now = datetime.now(timezone.utc).isoformat()
    r1 = requests.post(API_URL + '/tasks', json={
        'user_id': 'int-test',
        'payload': {'action': 'low'},
        'exec_time': now,
        'priority': 'low'
    })
    assert r1.status_code == 200
    low_id = r1.json()['task_id']

    # small pause to simulate later arrival
    time.sleep(0.2)

    # submit a high-priority task with same exec_time
    r2 = requests.post(API_URL + '/tasks', json={
        'user_id': 'int-test',
        'payload': {'action': 'high'},
        'exec_time': now,
        'priority': 'high'
    })
    assert r2.status_code == 200
    high_id = r2.json()['task_id']

    # connect to DB from the test runner (runs in container where 'postgres' is resolvable)
    conn = psycopg2.connect(DB_DSN)

    # wait for both to execute and compare timestamps
    high_started = wait_for_task_execution(conn, high_id, timeout=15)
    low_started = wait_for_task_execution(conn, low_id, timeout=15)

    assert high_started <= low_started, f"High priority started {high_started} after low {low_started}"

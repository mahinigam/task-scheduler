#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> checking python3.11"
if ! command -v python3.11 >/dev/null 2>&1; then
  echo "ERROR: python3.11 not found in PATH. Install Python 3.11 and retry." >&2
  exit 2
fi

echo "==> creating virtualenv (.venv)"
python3.11 -m venv .venv
. .venv/bin/activate

echo "==> upgrading pip/setuptools/wheel"
python -m pip install --upgrade pip setuptools wheel

echo "==> installing requirements"
pip install -r requirements.txt

echo "==> starting Docker Compose (build & up)"
docker compose up -d --build

echo "==> running test suite inside api container"
docker compose exec -T api pytest -q

echo "==> running smoke test (submit two tasks and show worker logs)"
# submit low then high (same exec_time)
docker compose exec -T api python - <<'PY'
import json, urllib.request, datetime
et = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
url = 'http://localhost:8000/tasks'
for pr in ('low','high'):
    body = {'user_id':'smoke-user','payload':{'x': pr},'exec_time':et,'priority':pr}
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=5) as resp:
        print(pr + ' ->', resp.read().decode())
PY

sleep 2

docker compose logs --no-color --tail 200 worker

echo "==> setup_and_run completed"

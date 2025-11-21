venv:
	python3.11 -m venv .venv

install: venv
	. .venv/bin/activate && python -m pip install --upgrade pip setuptools wheel && pip install -r requirements.txt

up:
	docker compose up -d --build

test:
	docker compose exec -T api pytest -q

smoke:
	# submit two tasks (low then high with identical exec_time) and show recent worker logs
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
	docker compose logs --no-color --tail 100 worker

clean:
	docker compose down --volumes --remove-orphans
	rm -rf .venv

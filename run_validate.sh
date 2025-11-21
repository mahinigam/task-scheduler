#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "Submitting immediate high task: $NOW"
curl -s -X POST "http://localhost:8000/tasks" -H "Content-Type: application/json" -d "{\"user_id\":\"user-1\",\"payload\":{\"action\":\"do_high\"},\"exec_time\":\"$NOW\",\"priority\":\"high\"}"

SLOW_TIME=$(date -u -v+10S +"%Y-%m-%dT%H:%M:%SZ")
echo "Submitting delayed low task: $SLOW_TIME"
curl -s -X POST "http://localhost:8000/tasks" -H "Content-Type: application/json" -d "{\"user_id\":\"user-1\",\"payload\":{\"action\":\"do_low\"},\"exec_time\":\"$SLOW_TIME\",\"priority\":\"low\"}"

echo "Submitting immediate medium task: $NOW"
curl -s -X POST "http://localhost:8000/tasks" -H "Content-Type: application/json" -d "{\"user_id\":\"user-2\",\"payload\":{\"action\":\"do_medium\"},\"exec_time\":\"$NOW\",\"priority\":\"medium\"}"

echo "Submitted tasks."

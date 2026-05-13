#!/usr/bin/env bash
# Replays the configured ratings slice in an endless loop.
# Stops on SIGINT (Ctrl+C). Each iteration logs how long it ran.
set -uo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if [ -d .venv ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

iter=0
trap 'echo "[$(date -u +%FT%TZ)] producer-loop interrupted, exiting"; exit 0' INT TERM

while true; do
  iter=$((iter + 1))
  start=$(date +%s)
  echo "[$(date -u +%FT%TZ)] === iteration $iter — starting producer ==="
  python producer/main.py
  rc=$?
  end=$(date +%s)
  echo "[$(date -u +%FT%TZ)] === iteration $iter done in $((end - start))s (exit=$rc); restarting in 3s ==="
  sleep 3
done

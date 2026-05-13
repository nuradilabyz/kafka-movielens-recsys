#!/usr/bin/env bash
# Останавливает pipeline. Запускать просто: ./stop_server.sh

set -uo pipefail

PLIST="$HOME/Library/LaunchAgents/com.nuradilabyz.kafka-pipeline.plist"

echo "=== Останавливаю launchd job ==="
launchctl unload "$PLIST" 2>/dev/null && echo "  launchd выгружен" || echo "  launchd уже не активен"

echo
echo "=== Останавливаю фоновые процессы ==="
pkill -f "scripts/producer_loop.sh"           2>/dev/null && echo "  убил producer_loop"     || true
pkill -f "consumers/recommender/main.py"      2>/dev/null && echo "  убил recommender"       || true
pkill -f "consumers/analytics/sink.py"        2>/dev/null && echo "  убил analytics_sink"    || true
pkill -f "producer/main.py"                   2>/dev/null && echo "  убил producer/main"     || true
pkill -f "caffeinate -i -m -s"                2>/dev/null && echo "  убил caffeinate"        || true

echo
echo "=== Останавливаю docker-compose ==="
docker compose stop 2>&1 | tail -5

echo
echo "✅ Остановлено. Mac снова может уйти в sleep."
echo "Чтобы запустить заново — ./run_server.sh"

#!/usr/bin/env bash
# Активирует pipeline как сервер на Mac.
# Запускать просто:  ./run_server.sh
#
# Что делает:
# 1. Убивает старые процессы (если они были запущены руками раньше)
# 2. Загружает launchd-плейст — он подхватит автостарт при каждом входе в систему
# 3. Запускает прямо сейчас
# 4. Показывает статус

set -uo pipefail

PLIST="$HOME/Library/LaunchAgents/com.nuradilabyz.kafka-pipeline.plist"

echo "=== [1/4] Останавливаю старые процессы ==="
pkill -f "scripts/producer_loop.sh"           2>/dev/null && echo "  убил producer_loop" || true
pkill -f "consumers/recommender/main.py"      2>/dev/null && echo "  убил recommender" || true
pkill -f "consumers/analytics/sink.py"        2>/dev/null && echo "  убил analytics_sink" || true
pkill -f "producer/main.py"                   2>/dev/null && echo "  убил producer/main" || true
pkill -f "caffeinate -i -m -s"                2>/dev/null && echo "  убил старый caffeinate" || true
sleep 2

echo
echo "=== [2/4] Выгружаю старый launchd (если был) ==="
launchctl unload "$PLIST" 2>/dev/null && echo "  выгружено" || echo "  не было ничего"
sleep 1

echo
echo "=== [3/4] Загружаю launchd и стартую pipeline ==="
launchctl load "$PLIST"
launchctl start com.nuradilabyz.kafka-pipeline
echo "  запущено: com.nuradilabyz.kafka-pipeline"

echo
echo "=== [4/4] Жду 30 сек пока всё поднимется и проверяю ==="
sleep 30

echo
echo "--- launchd job ---"
launchctl list | grep kafka-pipeline || echo "⚠️  launchd job не найден"

echo
echo "--- живые процессы ---"
ps aux | grep -E "(producer_loop|recommender/main|analytics/sink|caffeinate -i)" | grep -v grep | awk '{print "  "$11" "$12" "$13}'

echo
echo "--- docker контейнеры ---"
docker compose ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || echo "⚠️  docker compose недоступен"

echo
echo "============================================================"
echo "✅ Готово. Pipeline запущен и сам перезапустится после reboot."
echo
echo "Полезные команды:"
echo "  tail -f logs/launchd.out.log    # смотреть лог в реальном времени"
echo "  ./stop_server.sh                # остановить навсегда"
echo "============================================================"

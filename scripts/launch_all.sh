#!/usr/bin/env bash
# Поднимает весь pipeline: Docker compose + producer loop + recommender + analytics sink.
# Вызывается через launchd при логине пользователя. Логи пишутся в logs/.

set -uo pipefail

PROJECT_DIR="/Users/nuradilabyz/Desktop/kafka-project"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

cd "$PROJECT_DIR"

# Загружаем .env
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

log() {
  echo "[$(date -u +%FT%TZ)] $*"
}

log "=== Pipeline launcher starting ==="

# Шаг 1: дождаться Docker Desktop
log "Waiting for Docker daemon..."
for i in $(seq 1 60); do
  if /usr/local/bin/docker info >/dev/null 2>&1; then
    log "Docker daemon ready (after ${i}s)"
    break
  fi
  # запустим Docker Desktop если не запущен
  if [ "$i" = "1" ]; then
    /usr/bin/open -a Docker || true
  fi
  sleep 2
done

# Шаг 2: поднять docker-compose стек
log "Bringing up docker-compose stack..."
/usr/local/bin/docker compose up -d 2>&1 | tee -a "$LOG_DIR/docker.log"

# Подождём 30 сек чтобы Kafka точно был healthy
sleep 30

# Шаг 3: запретить sleep пока мы тут
# caffeinate -i (idle sleep prevention) — Mac не уйдёт в idle sleep пока этот процесс жив
log "Engaging caffeinate to prevent idle sleep..."
/usr/bin/caffeinate -i -m -s &
CAFFEINATE_PID=$!
log "caffeinate PID=$CAFFEINATE_PID"

# Шаг 4: запустить три Python-процесса
PYTHON="$PROJECT_DIR/.venv/bin/python"

log "Starting producer loop..."
"$PROJECT_DIR/scripts/producer_loop.sh" >> "$LOG_DIR/producer.log" 2>&1 &
PRODUCER_PID=$!

log "Starting recommender..."
"$PYTHON" "$PROJECT_DIR/consumers/recommender/main.py" >> "$LOG_DIR/recommender.log" 2>&1 &
RECOMMENDER_PID=$!

log "Starting analytics sink..."
"$PYTHON" "$PROJECT_DIR/consumers/analytics/sink.py" >> "$LOG_DIR/analytics.log" 2>&1 &
ANALYTICS_PID=$!

log "All started. PIDs: producer=$PRODUCER_PID recommender=$RECOMMENDER_PID analytics=$ANALYTICS_PID caffeinate=$CAFFEINATE_PID"

# Шаг 5: при остановке (SIGTERM) — аккуратно убить детей
cleanup() {
  log "Got signal, stopping children..."
  kill "$PRODUCER_PID" "$RECOMMENDER_PID" "$ANALYTICS_PID" "$CAFFEINATE_PID" 2>/dev/null || true
  wait
  log "Pipeline launcher stopped"
  exit 0
}
trap cleanup TERM INT

# Шаг 6: следим за процессами и рестартуем упавшие
while true; do
  if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
    log "Producer loop died, restarting..."
    "$PROJECT_DIR/scripts/producer_loop.sh" >> "$LOG_DIR/producer.log" 2>&1 &
    PRODUCER_PID=$!
  fi
  if ! kill -0 "$RECOMMENDER_PID" 2>/dev/null; then
    log "Recommender died, restarting..."
    "$PYTHON" "$PROJECT_DIR/consumers/recommender/main.py" >> "$LOG_DIR/recommender.log" 2>&1 &
    RECOMMENDER_PID=$!
  fi
  if ! kill -0 "$ANALYTICS_PID" 2>/dev/null; then
    log "Analytics sink died, restarting..."
    "$PYTHON" "$PROJECT_DIR/consumers/analytics/sink.py" >> "$LOG_DIR/analytics.log" 2>&1 &
    ANALYTICS_PID=$!
  fi
  sleep 30
done

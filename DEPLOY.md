# Деплой 24/7 на Oracle Cloud Always Free

Полная инструкция: как перенести pipeline (Kafka + ksqlDB + Qdrant +
producer/consumers) с твоего ноутбука на бесплатную ARM-VM в Oracle Cloud,
чтобы всё крутилось 24/7 без твоего участия.

**Результат:** Streamlit Cloud UI (публичная ссылка) видит живые обновления
независимо от того, включён твой ноут или нет.

---

## Часть 1. Регистрация в Oracle Cloud (~15 мин)

1. Открой https://signup.cloud.oracle.com/
2. Заполни форму:
   - Country: Kazakhstan
   - First/Last name: реальные
   - Email: рабочий
3. Home Region: **выбирай не Казахстан, а ближайший с capacity** для ARM.
   Рабочие варианты (проверены, ARM Ampere доступен):
   - **Germany Central (Frankfurt)** — обычно есть
   - **Netherlands Northwest (Amsterdam)**
   - **UK South (London)**
   - **US East (Ashburn)** — если EU не пускает

   ⚠️ Регион выбирается **один раз навсегда**, поменять нельзя без нового аккаунта.

4. Верификация по карте: вводи карту Kaspi/Halyk Visa или Mastercard.
   **Списаний не будет**, нужно только подтвердить.
   - Если казахская карта отлетает — попробуй через **VPN на EU** + ту же карту, или **Wise/Revolut**.
   - Иногда срабатывает: повторить через 24 часа.

5. После успешной регистрации зайди в консоль: https://cloud.oracle.com

---

## Часть 2. Создание ARM VM (~10 мин)

1. В консоли Oracle нажми меню (☰) → **Compute** → **Instances**
2. **Create Instance**
3. Настройки:

   | Поле | Значение |
   | --- | --- |
   | Name | `kafka-pipeline` |
   | Compartment | default (root) |
   | Image | **Canonical Ubuntu 22.04** |
   | Shape | **Change shape** → tab **Ampere** → **VM.Standard.A1.Flex** |
   | OCPU count | **4** |
   | Memory (GB) | **24** |
   | Network | оставь дефолтный VCN |
   | Public IPv4 address | **Assign a public IPv4 address** ✅ |
   | SSH keys | **Generate a key pair** → скачай **private key** (.key) и **public key** (.pub) |
   | Boot volume | **Specify a custom boot volume size** → **200 GB** |

4. **Create**

   Если выскочит «Out of host capacity» — попробуй другую Availability Domain
   (выпадашка вверху) или вернись через 1-2 часа.

5. Когда статус станет **Running** — скопируй **Public IP** (нужен для SSH).

---

## Часть 3. SSH-подключение и базовая настройка (~10 мин)

На своём Mac в терминале:

```bash
# Положи скачанный приватный ключ в стандартное место и поправь права
mv ~/Downloads/ssh-key-*.key ~/.ssh/oracle-kafka.key
chmod 600 ~/.ssh/oracle-kafka.key

# Подключись к VM (замени IP)
ssh -i ~/.ssh/oracle-kafka.key ubuntu@<PUBLIC_IP>
```

На VM (ты теперь внутри):

```bash
# Обнови систему
sudo apt update && sudo apt upgrade -y

# Поставь Docker
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER

# Поставь Python и git
sudo apt install -y python3-pip python3-venv git postgresql-client

# Перелогинься чтобы группа docker применилась
exit
```

Снова подключись:

```bash
ssh -i ~/.ssh/oracle-kafka.key ubuntu@<PUBLIC_IP>
docker --version  # должен показать версию без sudo
```

---

## Часть 4. Клонирование репо и подготовка (~5 мин)

На VM:

```bash
git clone https://github.com/nuradilabyz/kafka-movielens-recsys.git
cd kafka-movielens-recsys

# Создай .env (НЕ в репо, секреты)
cat > .env <<'EOF'
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_RATINGS=movielens.ratings.raw
KAFKA_TOPIC_TRENDING=movielens.trending.5m
KAFKA_TOPIC_ENRICHED=movielens.events.enriched
KAFKA_NUM_PARTITIONS=6

SPEED_FACTOR=259200
RATINGS_START_DATE=2019-01-01
RATINGS_END_DATE=2019-07-01

QDRANT_URL=http://localhost:6333
QDRANT_COLLECTION=movies
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
EMBEDDING_DIM=384

USER_VEC_EMA_ALPHA=0.85
TOP_N_RECS=10
NEON_UPSERT_BATCH_SIZE=200
NEON_UPSERT_INTERVAL_SEC=5

NEON_DSN='ВСТАВЬ_СЮДА_СТРОКУ_NEON'
EOF

# Открой и подставь NEON_DSN
nano .env
```

Замени `ВСТАВЬ_СЮДА_СТРОКУ_NEON` на свою реальную строку. **Ctrl+O**, **Enter**, **Ctrl+X**.

---

## Часть 5. Поднимаем Docker-стек (~10 мин)

```bash
# Поднимаем Kafka, ksqlDB, Qdrant и kafka-ui
docker compose up -d

# Подожди ~30 сек пока всё стартанёт
sleep 30
docker compose ps    # все должны быть Up или Healthy
```

Создай Python venv для скриптов:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip

# Базовые зависимости
pip install aiokafka pandas numpy psycopg2-binary qdrant-client \
            kafka-python requests sentence-transformers
```

---

## Часть 6. Заливаем данные и индекс (~30 мин)

```bash
source .venv/bin/activate
set -a && source .env && set +a

# 1. Скачать MovieLens 32M
python scripts/download_movielens.py
# (~3-5 мин)

# 2. Создать Kafka топики
python scripts/create_topics.py

# 3. Применить ksqlDB-запросы
docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 \
    < consumers/analytics/ksql_queries.sql

# 4. Построить векторный индекс в Qdrant (~15-20 мин на ARM)
python scripts/build_movie_embeddings.py

# 5. Применить схему Neon (уже применена, но на всякий)
psql "$NEON_DSN" -f scripts/init_neon_schema.sql

# 6. Построить UMAP карту для UI (нужна только если хочешь обновить .parquet
#    в репо; для cloud-UI уже всё есть в git)
pip install umap-learn scikit-learn
python scripts/build_movie_map_2d.py
```

---

## Часть 7. systemd: автостарт producer/consumers (~10 мин)

Чтобы Python-процессы перезапускались автоматически (после ребута, после сбоя),
оборачиваем их в systemd-units.

Создай файлы:

```bash
sudo tee /etc/systemd/system/kafka-producer.service > /dev/null <<EOF
[Unit]
Description=MovieLens producer loop
After=docker.service network-online.target
Wants=docker.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/kafka-movielens-recsys
EnvironmentFile=/home/ubuntu/kafka-movielens-recsys/.env
ExecStart=/home/ubuntu/kafka-movielens-recsys/scripts/producer_loop.sh
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/kafka-recommender.service > /dev/null <<EOF
[Unit]
Description=MovieLens recommender consumer
After=docker.service network-online.target
Wants=docker.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/kafka-movielens-recsys
EnvironmentFile=/home/ubuntu/kafka-movielens-recsys/.env
ExecStart=/home/ubuntu/kafka-movielens-recsys/.venv/bin/python consumers/recommender/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/kafka-analytics.service > /dev/null <<EOF
[Unit]
Description=MovieLens analytics sink
After=docker.service network-online.target
Wants=docker.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/kafka-movielens-recsys
EnvironmentFile=/home/ubuntu/kafka-movielens-recsys/.env
ExecStart=/home/ubuntu/kafka-movielens-recsys/.venv/bin/python consumers/analytics/sink.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
```

Запусти и включи автостарт:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now kafka-producer kafka-recommender kafka-analytics

# Проверь статус
sudo systemctl status kafka-producer kafka-recommender kafka-analytics
```

Лог в реальном времени:

```bash
sudo journalctl -u kafka-producer -f
# Ctrl+C чтобы выйти
```

---

## Часть 8. Verification (~5 мин)

На VM проверь что в Neon реально льются данные:

```bash
source .env
psql "$NEON_DSN" -c "
SELECT 'events_enriched' AS t, COUNT(*) FROM events_enriched
UNION ALL SELECT 'user_recs', COUNT(*) FROM user_recs
UNION ALL SELECT 'trending_movies', COUNT(*) FROM trending_movies;
"
```

Цифры должны расти каждые несколько секунд.

С Mac тоже можно проверить тот же запрос (Neon одна на всех).

---

## Часть 9. Деплой Streamlit Cloud (~5 мин)

Финальный штрих — публичная ссылка для портфолио.

1. Открой https://share.streamlit.io
2. **Sign in with GitHub** (`nuradilabyz`)
3. **Create app** → **Deploy a public app from GitHub**
4. Заполни:
   - Repository: `nuradilabyz/kafka-movielens-recsys`
   - Branch: `main`
   - Main file path: `ui/streamlit_app.py`
   - App URL: например `movielens-realtime`
5. **Advanced settings → Secrets** → вставь:
   ```toml
   NEON_DSN = "postgresql://neondb_owner:..."
   ```
6. **Deploy** → подожди 2-3 минуты сборки

Готово — публичный URL вида `https://movielens-realtime.streamlit.app` будет
работать **24/7**, читать данные из Neon, которые льёт ARM-VM в Oracle Cloud.

---

## Что делать если что-то ломается

| Симптом | Что проверить |
| --- | --- |
| `docker compose ps` — какой-то контейнер падает | `docker compose logs <имя>` |
| Producer не льёт | `sudo journalctl -u kafka-producer -n 100` |
| Метрика «обновлено: 10 мин назад» в UI | Один из консьюмеров умер: `sudo systemctl status kafka-recommender` |
| Streamlit Cloud не открывается | Проверь Secrets — NEON_DSN правильный? |
| VM «out of capacity» в Oracle | Попробуй другой Availability Domain или регион |
| Oracle прислал «idle reclaim warning» | Запусти что-нибудь, чтобы CPU не был < 20%. Наш producer уже жуёт CPU, должно хватить |

## Команды для управления

```bash
# Остановить весь стек на VM
sudo systemctl stop kafka-producer kafka-recommender kafka-analytics
docker compose down

# Запустить всё обратно
docker compose up -d
sudo systemctl start kafka-producer kafka-recommender kafka-analytics

# Обновить код из репо
cd ~/kafka-movielens-recsys
git pull
sudo systemctl restart kafka-producer kafka-recommender kafka-analytics

# Логи отдельных сервисов
sudo journalctl -u kafka-producer -f
sudo journalctl -u kafka-recommender -f
sudo journalctl -u kafka-analytics -f

# Состояние Docker
docker compose ps
docker stats     # CPU/RAM по контейнерам
```

## Ожидаемое потребление ресурсов (на 4 vCPU / 24 GB)

| Компонент | RAM | CPU |
| --- | --- | --- |
| Kafka | ~700 MB | низкое |
| ksqlDB | ~600 MB | низкое |
| Schema Registry | ~400 MB | минимум |
| Qdrant | ~300 MB | низкое (HNSW lazy) |
| kafka-ui | ~200 MB | минимум |
| Producer | ~600 MB (читает CSV в RAM) | 1 vCPU |
| Recommender | ~400 MB | 1 vCPU |
| Analytics sink | ~400 MB | 0.5 vCPU |
| **Итого** | **~3.5 GB / 24 GB** ✅ | **~3 vCPU / 4** ✅ |

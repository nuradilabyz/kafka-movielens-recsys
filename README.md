# 🎬 MovieLens Real-Time Recommender on Kafka

> Production-style streaming pipeline that replays **32 million** historical movie ratings into **Apache Kafka**, computes trending windows in **ksqlDB**, maintains per-user vector preferences in **Qdrant**, and serves it all through a live **Streamlit Cloud** dashboard.

---

## 🔗 Try it now

| | |
| --- | --- |
| 🌐 **Live demo** | **https://kafka-movielens.streamlit.app/** |
| 💻 Source code | https://github.com/nuradilabyz/kafka-movielens-recsys |
| 📊 Dataset | [MovieLens 32M](https://grouplens.org/datasets/movielens/32m/) — real ratings, 1995–2023 |

The dashboard auto-refreshes every 5 seconds. If counters look stale, the upstream pipeline on my laptop is sleeping — the snapshot in Postgres still demonstrates the architecture. Open the demo on a desktop browser for the best experience (UI is in Russian).

---

## What this is, in one paragraph

A real-time recommendation engine built around **Apache Kafka**. A producer replays the **MovieLens 32M** dataset into a partitioned topic at a configurable wall-clock speed (one day of historical events per second, by default). Two stateful consumers process the stream: one keeps a 384-dimensional "taste vector" per user and queries Qdrant for top-N nearest movies in real time; the other enriches events with metadata and persists them. **ksqlDB** computes tumbling 5-minute windows of trending movies as a streaming SQL pipeline. Everything lands in **Neon Postgres**, and a **Streamlit Cloud** UI reads it for the public dashboard.

## Architecture

```
LOCAL (docker-compose on a Mac)                                          CLOUD
──────────────────────────────────────────────────────────────         ──────────────────

ratings.csv (32M rows) ──► producer/main.py (aiokafka)
                              · replays in event-timestamp order
                              · configurable speed factor
                              · key = user_id
                              ▼
                ┌────────────────────────────────────────┐
                │  Kafka topic:  movielens.ratings.raw   │  (6 partitions)
                └─────────────────┬──────────────────────┘
                                  │
       ┌──────────────────────────┼──────────────────────────┐
       ▼                          ▼                          ▼
  ksqlDB streams           consumers/recommender       consumers/analytics
   · tumbling 5-min         · per-user EMA vector       · enriches with movies.csv
   · tumbling 1-hour        · Qdrant top-N search       · genre windows
       │                          │                          │
       ▼                          ▼                          ▼
                  ┌──────────────────────────────────┐
                  │     Neon Postgres (managed)      │ ◄── public endpoint
                  │  user_recs, trending_movies,     │
                  │  events_enriched, pipeline_metrics                 │
                  └─────────────────┬────────────────┘
                                    │
                                    ▼
                         Streamlit Community Cloud
                         · "Тренды" — live windowed top-N
                         · "Рекомендации" — UMAP taste map
                         · "Состояние" — pipeline health
```

## What it demonstrates (Data Engineer skills)

| Concept | Where in this project |
| --- | --- |
| **Partitioned event streaming** | producer keys by `user_id` → 6-partition topic → per-user ordering preserved across consumers |
| **Stateful stream processing** | recommender keeps an in-memory EMA vector store (Faust-style table), persists user_vec on flush |
| **Streaming SQL** | ksqlDB `WINDOW TUMBLING (SIZE 5 MINUTES)` and `1 HOUR` — windowed aggregates without a Streams app |
| **Consumer groups** | analytics and recommender are independent groups → can scale horizontally with more partitions |
| **Schema management** | Schema Registry deployed (Confluent OSS); wire format starts as JSON, Avro path documented |
| **Vector search at write-time** | every rating event triggers Qdrant nearest-neighbour query against `all-MiniLM-L6-v2` embeddings (384-d) |
| **Idempotent state** | `user_recs.user_id` is the PK; per-user UPSERT bounds the table regardless of throughput |
| **Operational metrics** | `pipeline_metrics` table tracks per-component throughput + last-seen — exposed in UI |
| **Wall-clock vs event-time windows** | deliberately uses Kafka record timestamp for windowing (demo-time) vs the historical `ts` field (event-time) — both stored, trade-off documented |
| **Hybrid deployment** | compute-heavy parts local (Kafka, ksqlDB, Qdrant, consumers); durable state managed (Neon); UI managed (Streamlit Cloud) |

## Stack

| Layer | Choice | Why |
| --- | --- | --- |
| Broker | **Apache Kafka 7.6** (KRaft, no ZK) | de facto industry standard for event streaming |
| Streaming SQL | **ksqlDB** | windowed aggregates declaratively, no Streams Java boilerplate |
| Vector store | **Qdrant 1.11** | fast HNSW, Python client, runs locally |
| Producer + consumers | **Python 3.11 + aiokafka** | async I/O, succinct, easy to read on review |
| Sink DB | **Neon Postgres** | free managed Postgres, pgvector-capable, public endpoint |
| UI | **Streamlit Community Cloud** | free hosting + GitHub auto-deploy |
| Embeddings | **`sentence-transformers/all-MiniLM-L6-v2`** | 384-d, ~80 MB, CPU-friendly |
| 2D projection | **UMAP (cosine)** | clean cluster separation for the "taste map" visualization |
| Schema | **Confluent Schema Registry** | wired in compose, reserved for Avro migration phase |
| Local UI for Kafka | **provectus/kafka-ui** | topic inspection, consumer-group offsets |

## Highlights (with code pointers)

- **Pure replay producer** — sorts by `ts`, sleeps `(t_event - t_event_prev) / SPEED_FACTOR` between sends: [`producer/main.py`](producer/main.py)
- **Recommender with EMA + Qdrant + Neon UPSERT** — fully async, batched flushes: [`consumers/recommender/main.py`](consumers/recommender/main.py)
- **ksqlDB streaming SQL** — windowed tables, key extraction: [`consumers/analytics/ksql_queries.sql`](consumers/analytics/ksql_queries.sql)
- **UMAP taste map** with kNN-based projection of user vectors: [`scripts/build_movie_map_2d.py`](scripts/build_movie_map_2d.py), [`ui/taste_map.py`](ui/taste_map.py)
- **Watchdog launcher** that auto-restarts crashed components and prevents Mac sleep via `caffeinate`: [`scripts/launch_all.sh`](scripts/launch_all.sh)

## Run locally

Prereqs: Docker Desktop, Python 3.11+, `psql`.

```bash
git clone https://github.com/nuradilabyz/kafka-movielens-recsys.git
cd kafka-movielens-recsys

# 1. Configure
cp .env.example .env
# Put your Neon DSN into .env

# 2. Bring up Kafka + ksqlDB + Qdrant + kafka-ui
docker compose up -d

# 3. Install Python deps
python3 -m venv .venv && source .venv/bin/activate
pip install -r ui/requirements.txt \
            aiokafka kafka-python sentence-transformers qdrant-client requests

# 4. Bootstrap data
set -a && source .env && set +a
python scripts/download_movielens.py             # ~5 min
psql "$NEON_DSN" -f scripts/init_neon_schema.sql
python scripts/create_topics.py
python scripts/build_movie_embeddings.py         # ~15-20 min
docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 \
    < consumers/analytics/ksql_queries.sql

# 5. Run pipeline (producer + 2 consumers as background services)
./run_server.sh                                  # macOS: loads a launchd plist

# 6. UI (locally)
streamlit run ui/streamlit_app.py
```

## Project layout

```
.
├── docker-compose.yml              # Kafka, ksqlDB, Schema Registry, Qdrant, kafka-ui
├── producer/                       # aiokafka producer (replay)
├── consumers/
│   ├── recommender/                # raw aiokafka + Qdrant + Neon
│   └── analytics/                  # ksql_queries.sql + Python sink for Postgres
├── scripts/
│   ├── download_movielens.py       # fetch & unpack dataset
│   ├── build_movie_embeddings.py   # 87K movies → Qdrant
│   ├── build_movie_map_2d.py       # UMAP projection + raw vectors for kNN
│   ├── create_topics.py            # idempotent topic creation
│   ├── init_neon_schema.sql        # 5 tables + indexes
│   └── producer_loop.sh            # restarts producer when slice ends
├── ui/                             # Streamlit app (Russian)
│   ├── streamlit_app.py            # entry point
│   ├── pages/                      # Тренды / Рекомендации / Состояние
│   ├── taste_map.py                # kNN projection helper
│   ├── movie_map.parquet           # precomputed UMAP coords (14K rows)
│   └── movie_vectors_384d.npy      # raw vectors for online kNN projection
├── run_server.sh / stop_server.sh  # one-command Mac autostart
└── compass_artifact_*.md           # research artefact — why MovieLens won (ADR)
```

## Design decisions (the trade-offs)

- **Why MovieLens 32M.** Real timestamps, real users, ~1 GB unpacked — the cleanest open dataset for a Kafka replay story. Ratings are framed as *view-completion events* per the [research artefact](compass_artifact_wf-63fce55a-9753-48cb-b97a-5ff85f7af02e_text_markdown.md), which evaluated 7 candidate projects before this one was chosen.
- **Why raw `aiokafka` instead of Faust.** The original Robinhood Faust is archived; `faust-streaming` fork works but adds DSL overhead. Raw `aiokafka` consumer + a Python dict demonstrates the same Kafka primitives more readably.
- **Why ksqlDB for trending but Python for recs.** Windowed aggregates are one-liners in ksqlDB. EMA updates + vector lookups aren't idiomatic SQL — so they live in a stateful Python consumer.
- **Why wall-clock windowing (not event-time).** Producer replays 2019 events at `SPEED_FACTOR=259200` (3 days per second). Event-time windows would all be in 2019 — meaningless for a live demo. Wall-clock windows make "trending in last 5 minutes" map to demo time. The original `ts` is still stored on every row in `events_enriched`.
- **Why hybrid local/cloud.** Managed Kafka (Confluent Cloud, MSK, Aiven) costs $20–100+/month. The whole stack runs comfortably on a laptop and writes durable state to Neon (free tier). The public Streamlit URL serves the latest snapshot 24/7; live updates happen when the laptop is on.

## What's deliberately out of scope (next steps)

- Switch wire format to **Avro** and version with Schema Registry
- **Kafka Connect** JDBC Sink instead of the Python `analytics/sink.py`
- **Prometheus + Grafana** for consumer lag and throughput dashboards
- **Exactly-once** semantics (transactional producer + read-committed consumer)
- Move broker to a free always-on VM (e.g., Oracle Cloud Always Free ARM, if signup goes through)

## License

MIT for the code in this repo. MovieLens dataset has its own [non-commercial license](https://files.grouplens.org/datasets/movielens/ml-32m-README.html).

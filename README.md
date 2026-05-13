# MovieLens Real-Time Recommender on Kafka

Portfolio-grade streaming pipeline: a Python producer replays MovieLens 32M
ratings into Kafka in event-timestamp order, a stateful consumer maintains a
per-user EMA vector and queries Qdrant for top-N nearest movies, ksqlDB computes
tumbling-window trending movies, and everything lands in Neon Postgres so a
Streamlit Cloud UI can show the live state from anywhere.

Dataset choice and the seven alternatives we considered are documented in the
ADR-style research file [compass_artifact_…md](compass_artifact_wf-63fce55a-9753-48cb-b97a-5ff85f7af02e_text_markdown.md).

## Architecture

```
LOCAL (docker-compose)                                  CLOUD
──────────────────────────────────────────────────      ──────────────────

ratings.csv ─► producer/main.py (aiokafka)
                 sorted by event ts, speed_factor
                 │  key = user_id
                 ▼
            ┌──────────────────────────────────┐
            │ topic: movielens.ratings.raw     │ (6 partitions)
            └────────────┬─────────────────────┘
                         │
       ┌─────────────────┼──────────────────────────┐
       ▼                 ▼                          ▼
  ksqlDB             consumers/recommender    consumers/analytics
  trending_5m,       Qdrant top-N per user    enrichment + genre
  user_activity_1h   → user_recs in Neon      windows → trending_movies,
                                              top_genres, events_enriched
                         │                          │
                         ▼                          ▼
                   ┌────────────────────────────────────┐
                   │       Neon Postgres (cloud)        │
                   └────────────────────────────────────┘
                                  │
                                  ▼
                       Streamlit Community Cloud
                       (reads Neon, auto-refresh)
```

## Stack

| Layer | Tool | Where |
| --- | --- | --- |
| Dataset | MovieLens 32M (`ratings.csv`, `movies.csv`, `tags.csv`) | local `data/` |
| Broker | Kafka 7.6 in KRaft mode (Confluent OSS image) | local Docker |
| Schema / streaming SQL | Schema Registry + ksqlDB | local Docker |
| Vector store | Qdrant 1.11 | local Docker |
| Producer | Python + `aiokafka` | local |
| Consumers | Python + `aiokafka` + `qdrant-client` + `psycopg2` | local |
| Sink DB | Neon Postgres (free tier) | cloud |
| UI | Streamlit Community Cloud | cloud |
| Kafka admin UI | `provectuslabs/kafka-ui` at `localhost:8080` | local |

## Layout

```
.
├── docker-compose.yml          # Kafka, Schema Registry, ksqlDB, Qdrant, kafka-ui
├── .env.example                # copy to .env and fill NEON_DSN, etc.
├── scripts/
│   ├── download_movielens.py   # fetch + unzip ml-32m
│   ├── init_neon_schema.sql    # CREATE TABLE … in Neon
│   ├── create_topics.py        # create Kafka topics
│   └── build_movie_embeddings.py  # encode movies → Qdrant
├── producer/                   # aiokafka producer (replay)
├── consumers/
│   ├── recommender/            # raw aiokafka + Qdrant + Neon upsert
│   └── analytics/              # ksql_queries.sql + sink.py for Postgres
└── ui/                         # Streamlit app (entrypoint + 3 pages)
```

## Quickstart

### 0. Prereqs
- Docker Desktop with ≥ 6 GB RAM
- Python 3.11+
- A free Neon project ([neon.tech](https://neon.tech)) — copy the connection
  string and put it into `.env` as `NEON_DSN`
- A GitHub repo + a free [Streamlit Community Cloud](https://share.streamlit.io)
  account if you want a public UI URL

```bash
cp .env.example .env
# edit NEON_DSN and (optionally) RATINGS_START_DATE / RATINGS_END_DATE / SPEED_FACTOR
```

### 1. Bring up local infra

```bash
docker compose up -d
docker compose ps   # all services should report healthy/running
```

UIs:
- Kafka UI – http://localhost:8080
- Qdrant dashboard – http://localhost:6333/dashboard

### 2. Download the dataset and prepare Neon

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r ui/requirements.txt qdrant-client sentence-transformers \
            aiokafka pandas numpy kafka-python lz4

python scripts/download_movielens.py
psql "$NEON_DSN" -f scripts/init_neon_schema.sql
python scripts/create_topics.py
```

### 3. Index movies into Qdrant

```bash
python scripts/build_movie_embeddings.py
# ~87K vectors, ~5–15 min depending on hardware
```

### 4. Apply ksqlDB streaming queries

```bash
docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 \
    < consumers/analytics/ksql_queries.sql
```

### 5. Start producer + consumers

In three terminals (or run via `docker compose run` if you build the service images):

```bash
# Terminal 1 — producer
set -a; source .env; set +a
python producer/main.py

# Terminal 2 — recommender (Qdrant top-N → Neon)
set -a; source .env; set +a
python consumers/recommender/main.py

# Terminal 3 — analytics sink (ksqlDB topics + enrichment → Neon)
set -a; source .env; set +a
python consumers/analytics/sink.py
```

### 6. Run the UI locally (optional)

```bash
cd ui
pip install -r requirements.txt
NEON_DSN="$NEON_DSN" streamlit run streamlit_app.py
```

### 7. Deploy the UI

1. Push the repo to GitHub.
2. In Streamlit Community Cloud, point a new app at `ui/streamlit_app.py`.
3. In the app's **Secrets**, add: `NEON_DSN = "postgresql://..."`.
4. Deploy — the public URL works even when your laptop is off (it just shows
   the last snapshot until the local stack runs again).

## Verification

End-to-end smoke test, in order:

1. `docker compose ps` — every service is healthy/running.
2. `data/ml-32m/ratings.csv` exists and is ≈ 900 MB.
3. `psql "$NEON_DSN" -c '\dt'` — five tables created.
4. Qdrant dashboard shows ~87 000 points in the `movies` collection.
5. Kafka UI shows topics `movielens.ratings.raw`, `movielens.trending.5m`,
   `movielens.events.enriched` each with 6 partitions.
6. Run the producer with a 1-month slice (e.g. `RATINGS_START_DATE=2019-01-01`,
   `RATINGS_END_DATE=2019-02-01`, `SPEED_FACTOR=86400`) and watch the offset
   on `movielens.ratings.raw` climb in Kafka UI.
7. Recommender logs show `processed=…` and `flushed N user_recs rows`; a
   `SELECT count(*) FROM user_recs` in Neon grows.
8. Analytics sink logs show `flushed events=… trending_windows=…`; the
   `events_enriched`, `trending_movies`, `top_genres` tables in Neon populate.
9. Open the Streamlit URL — the **Trending** page shows the current 5-minute
   window and refreshes every 5s; **Recs for user** lets you pick a recently
   updated user and see their top-10.
10. Stop the producer; wait one minute; the UI keeps working and shows the
    last snapshot from Neon — confirming the hybrid local/cloud bridge.

## Design decisions and risks

- **Why MovieLens 32M.** Cleanest open dataset for a Kafka-replay story:
  real timestamps, real users, real ratings, ~1 GB unpacked. Trade-off: ratings
  are not literal "watch events" — we frame them as view-completion events.
  Full alternatives discussion in the research file.
- **Why hybrid deployment.** Managed Kafka costs $20–100+/month; the laptop is
  free. The UI needs to be reachable 24/7 for a portfolio link, so consumers
  write to Neon (free tier), Streamlit Cloud reads Neon. When the laptop is
  off, the UI shows the last snapshot, which is acceptable for a portfolio.
- **Why raw `aiokafka` instead of Faust.** The plan called for Faust; the
  original Robinhood project is archived. The `faust-streaming` fork works but
  adds a DSL on top of features (consumer group, partitioned state) that are
  trivial to express directly with `aiokafka` + a dict. Less moving parts;
  same demo.
- **Why ksqlDB for analytics but Python for recs.** Tumbling-window aggregates
  are a SQL one-liner in ksqlDB. The recommender needs vector lookups and
  per-user EMA — neither is idiomatic in SQL, so it's a stateful Python
  consumer that owns those operations.
- **Why no JDBC Sink Connector.** Avoiding the Kafka Connect runtime keeps the
  compose file smaller. A small Python consumer doing batched `INSERT … ON
  CONFLICT` is enough for the throughput of a replayed historical month.
- **Neon free-tier budget.** 0.5 GB storage + 100 compute-hours/month.
  Strategy: `user_recs` is keyed by user (UPSERT — no row growth);
  `events_enriched` is capped at `EVENTS_TAIL_MAX` (5 000 by default) with a
  trailing DELETE per flush; `trending_movies` is a small per-window table.
- **Schema evolution.** JSON over the wire today; Avro + Schema Registry is a
  phase-2 upgrade — Schema Registry is already in the compose file ready for
  it.
- **Secrets.** `.env` is gitignored. Neon DSN goes into `.env` locally and
  into Streamlit Cloud **Secrets** for the deployed UI; never commit it.

## What's next (out of scope for v1)

- Switch wire format to Avro and register a schema with Schema Registry.
- Add Kafka Connect with the Confluent JDBC Sink — write straight from
  ksqlDB topics to Neon, retire `analytics/sink.py`.
- Prometheus + Grafana for consumer lag and throughput dashboards.
- A second domain (e.g. ratings + tags) joined as a stream-stream join.
- Exactly-once: switch the producer to transactional mode and the consumer
  to read-committed.

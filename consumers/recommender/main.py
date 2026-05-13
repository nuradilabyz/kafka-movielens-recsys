"""Stateful Kafka consumer that maintains per-user vectors and writes top-N recs to Neon.

For each rating event:
  1. Look up the movie vector in Qdrant.
  2. Update the user vector as EMA: user_vec = alpha * user_vec + (1 - alpha) * movie_vec.
  3. Search Qdrant for nearest neighbours, excluding movies the user has already rated.
  4. Upsert the top-N recommendations into Neon Postgres `user_recs`.

The plan originally specified Faust; we use raw aiokafka instead because Faust is
unmaintained upstream and adds complexity without changing the demo's surface area.
Partitioning by user_id (set by the producer) gives us a per-user ordered stream
inside each partition.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from collections import defaultdict, deque
from typing import Deque, Dict

import numpy as np
import psycopg2
import psycopg2.extras
from aiokafka import AIOKafkaConsumer
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

LOG = logging.getLogger("recommender")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_RATINGS", "movielens.ratings.raw")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "movielens-recommender")

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "movies")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "384"))

NEON_DSN = os.getenv("NEON_DSN")
ALPHA = float(os.getenv("USER_VEC_EMA_ALPHA", "0.85"))
TOP_N = int(os.getenv("TOP_N_RECS", "10"))
BATCH_SIZE = int(os.getenv("NEON_UPSERT_BATCH_SIZE", "200"))
BATCH_INTERVAL = float(os.getenv("NEON_UPSERT_INTERVAL_SEC", "5"))
HISTORY_PER_USER = int(os.getenv("USER_HISTORY_LIMIT", "200"))
LOG_EVERY = int(os.getenv("CONSUMER_LOG_EVERY", "1000"))


class State:
    def __init__(self) -> None:
        self.user_vecs: Dict[int, np.ndarray] = {}
        self.user_history: Dict[int, Deque[int]] = defaultdict(
            lambda: deque(maxlen=HISTORY_PER_USER)
        )
        self.pending_recs: Dict[int, list[dict]] = {}
        self.last_flush = time.monotonic()
        self.processed = 0


async def _flush(state: State, conn) -> None:
    if not state.pending_recs:
        state.last_flush = time.monotonic()
        return
    rows = [
        (
            user_id,
            json.dumps(recs),
            state.user_vecs.get(user_id),
        )
        for user_id, recs in state.pending_recs.items()
    ]
    state.pending_recs.clear()
    state.last_flush = time.monotonic()

    def _do_upsert() -> None:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO user_recs (user_id, recs, user_vec, updated_at) VALUES %s "
                "ON CONFLICT (user_id) DO UPDATE SET "
                "    recs = EXCLUDED.recs, "
                "    user_vec = EXCLUDED.user_vec, "
                "    updated_at = EXCLUDED.updated_at",
                [
                    (u, r, vec.tolist() if vec is not None else None)
                    for u, r, vec in rows
                ],
                template="(%s, %s::jsonb, %s::real[], NOW())",
            )
            cur.execute(
                """
                INSERT INTO pipeline_metrics (component, messages_total, last_event_ts, updated_at)
                VALUES (%s, %s, NOW(), NOW())
                ON CONFLICT (component) DO UPDATE SET
                    messages_total = pipeline_metrics.messages_total + EXCLUDED.messages_total,
                    last_event_ts = EXCLUDED.last_event_ts,
                    updated_at = NOW()
                """,
                ("recommender", len(rows)),
            )
        conn.commit()

    await asyncio.get_running_loop().run_in_executor(None, _do_upsert)
    LOG.info("flushed %d user_recs rows", len(rows))


def _fetch_movie_vector(qdrant: QdrantClient, movie_id: int) -> np.ndarray | None:
    try:
        result = qdrant.retrieve(
            collection_name=COLLECTION,
            ids=[movie_id],
            with_vectors=True,
            with_payload=False,
        )
    except Exception as exc:  # pragma: no cover - defensive
        LOG.warning("qdrant retrieve failed for movie_id=%s: %s", movie_id, exc)
        return None
    if not result:
        return None
    vec = result[0].vector
    if vec is None:
        return None
    return np.asarray(vec, dtype=np.float32)


def _search_recs(
    qdrant: QdrantClient, user_vec: np.ndarray, seen: set[int]
) -> list[dict]:
    result = qdrant.query_points(
        collection_name=COLLECTION,
        query=user_vec.tolist(),
        limit=TOP_N + len(seen),
        with_payload=True,
    )
    out: list[dict] = []
    for hit in result.points:
        movie_id = int(hit.id)
        if movie_id in seen:
            continue
        payload = hit.payload or {}
        out.append(
            {
                "movie_id": movie_id,
                "title": payload.get("title"),
                "genres": payload.get("genres"),
                "score": float(hit.score),
            }
        )
        if len(out) >= TOP_N:
            break
    return out


async def _consume() -> None:
    if not NEON_DSN:
        raise SystemExit("NEON_DSN env var is required")

    qdrant = QdrantClient(url=QDRANT_URL)
    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = False
    state = State()

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: int(k.decode("utf-8")) if k else None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    LOG.info("consuming %s as group=%s", TOPIC, GROUP_ID)

    try:
        while not stop.is_set():
            batch = await consumer.getmany(timeout_ms=1000, max_records=500)
            now = time.monotonic()
            for _, messages in batch.items():
                for msg in messages:
                    event = msg.value
                    user_id = int(event["user_id"])
                    movie_id = int(event["movie_id"])

                    movie_vec = _fetch_movie_vector(qdrant, movie_id)
                    if movie_vec is None:
                        continue

                    prev = state.user_vecs.get(user_id)
                    if prev is None:
                        user_vec = movie_vec
                    else:
                        user_vec = ALPHA * prev + (1.0 - ALPHA) * movie_vec
                    norm = np.linalg.norm(user_vec)
                    if norm > 0:
                        user_vec = user_vec / norm
                    state.user_vecs[user_id] = user_vec

                    history = state.user_history[user_id]
                    history.append(movie_id)
                    recs = _search_recs(qdrant, user_vec, set(history))
                    if recs:
                        state.pending_recs[user_id] = recs

                    state.processed += 1
                    if state.processed % LOG_EVERY == 0:
                        LOG.info(
                            "processed=%d users_tracked=%d pending=%d",
                            state.processed,
                            len(state.user_vecs),
                            len(state.pending_recs),
                        )

            should_flush = (
                len(state.pending_recs) >= BATCH_SIZE
                or (now - state.last_flush) >= BATCH_INTERVAL
            )
            if should_flush:
                await _flush(state, conn)
                await consumer.commit()
        LOG.info("stop signal received; flushing final batch")
        await _flush(state, conn)
        await consumer.commit()
    finally:
        await consumer.stop()
        conn.close()


def main() -> int:
    asyncio.run(_consume())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Sink ksqlDB analytics topics + raw ratings into Neon Postgres.

Consumes three topics:
  - movielens.trending.5m       → table trending_movies
  - movielens.user_activity.1h  → (skipped by default; left as exercise)
  - movielens.ratings.raw       → table events_enriched (tail of recent events for the UI)

Enrichment: movies.csv is loaded into memory once for movie_id → (title, genres).
A simple in-memory `genre_window` aggregator computes per-genre counts and
flushes into the `top_genres` table on the same cadence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from aiokafka import AIOKafkaConsumer

LOG = logging.getLogger("analytics-sink")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

ROOT = Path(__file__).resolve().parent.parent.parent
MOVIES_CSV = Path(os.getenv("MOVIES_CSV_PATH", ROOT / "data/ml-32m/movies.csv"))

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RATINGS = os.getenv("KAFKA_TOPIC_RATINGS", "movielens.ratings.raw")
TOPIC_TRENDING = os.getenv("KAFKA_TOPIC_TRENDING", "movielens.trending.5m")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "movielens-analytics-sink")

NEON_DSN = os.getenv("NEON_DSN")
FLUSH_INTERVAL_SEC = float(os.getenv("ANALYTICS_FLUSH_SEC", "5"))
EVENTS_TAIL_MAX = int(os.getenv("EVENTS_TAIL_MAX", "5000"))
GENRE_WINDOW_SEC = int(os.getenv("GENRE_WINDOW_SEC", "3600"))


class Buffers:
    def __init__(self) -> None:
        self.events: list[tuple] = []
        self.trending: dict[tuple, tuple] = {}
        self.genre_counts: dict[tuple, tuple[int, float]] = defaultdict(lambda: (0, 0.0))


def _load_movies() -> dict[int, tuple[str, str]]:
    if not MOVIES_CSV.exists():
        LOG.warning("movies.csv not found at %s; titles/genres will be NULL", MOVIES_CSV)
        return {}
    df = pd.read_csv(MOVIES_CSV)
    df["genres"] = df["genres"].fillna("").str.replace("|", ", ", regex=False)
    return {int(r.movieId): (r.title, r.genres) for r in df.itertuples(index=False)}


def _window_bucket(epoch_sec: int) -> tuple[int, int]:
    start = (epoch_sec // GENRE_WINDOW_SEC) * GENRE_WINDOW_SEC
    return start, start + GENRE_WINDOW_SEC


def _to_dt(epoch_sec: int) -> datetime:
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc)


async def _flush(buffers: Buffers, conn) -> None:
    events = buffers.events
    trending = buffers.trending
    genre_counts = buffers.genre_counts
    buffers.events = []
    buffers.trending = {}
    buffers.genre_counts = defaultdict(lambda: (0, 0.0))

    def _do() -> None:
        with conn.cursor() as cur:
            if events:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO events_enriched "
                    "(user_id, movie_id, title, genres, rating, event_ts) VALUES %s",
                    events,
                    page_size=500,
                )
                cur.execute(
                    "DELETE FROM events_enriched WHERE id IN ("
                    "  SELECT id FROM events_enriched ORDER BY id DESC OFFSET %s)",
                    (EVENTS_TAIL_MAX,),
                )

            if trending:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO trending_movies "
                    "(window_start, window_end, movie_id, title, genres, "
                    " rating_count, avg_rating) VALUES %s "
                    "ON CONFLICT (window_start, movie_id) DO UPDATE SET "
                    "  window_end = EXCLUDED.window_end, "
                    "  title = EXCLUDED.title, "
                    "  genres = EXCLUDED.genres, "
                    "  rating_count = EXCLUDED.rating_count, "
                    "  avg_rating = EXCLUDED.avg_rating",
                    list(trending.values()),
                    page_size=500,
                )

            if genre_counts:
                rows = [
                    (
                        _to_dt(ws),
                        _to_dt(we),
                        genre,
                        count,
                        rating_sum / count if count else None,
                    )
                    for (ws, we, genre), (count, rating_sum) in genre_counts.items()
                ]
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO top_genres "
                    "(window_start, window_end, genre, rating_count, avg_rating) VALUES %s "
                    "ON CONFLICT (window_start, genre) DO UPDATE SET "
                    "  window_end = EXCLUDED.window_end, "
                    "  rating_count = top_genres.rating_count + EXCLUDED.rating_count, "
                    "  avg_rating = ("
                    "    (top_genres.avg_rating * top_genres.rating_count) + "
                    "    (EXCLUDED.avg_rating * EXCLUDED.rating_count)"
                    "  ) / (top_genres.rating_count + EXCLUDED.rating_count)",
                    rows,
                    page_size=500,
                )

            cur.execute(
                "INSERT INTO pipeline_metrics (component, messages_total, last_event_ts, updated_at) "
                "VALUES (%s, %s, NOW(), NOW()) "
                "ON CONFLICT (component) DO UPDATE SET "
                "  messages_total = pipeline_metrics.messages_total + EXCLUDED.messages_total, "
                "  last_event_ts = EXCLUDED.last_event_ts, "
                "  updated_at = NOW()",
                ("analytics_sink", len(events)),
            )
        conn.commit()

    await asyncio.get_running_loop().run_in_executor(None, _do)
    if events or trending or genre_counts:
        LOG.info(
            "flushed events=%d trending_windows=%d genre_windows=%d",
            len(events),
            len(trending),
            len(genre_counts),
        )


def _handle_rating(buffers: Buffers, movies: dict, event: dict, kafka_ts_ms: int) -> None:
    user_id = int(event["user_id"])
    movie_id = int(event["movie_id"])
    rating = float(event["rating"])
    event_ts = int(event["ts"])
    title, genres = movies.get(movie_id, (None, None))

    # events_enriched keeps the original historical timestamp so the UI shows
    # "this rating was made on 2019-..."
    buffers.events.append(
        (user_id, movie_id, title, genres, rating, _to_dt(event_ts))
    )

    # genre windows use Kafka record timestamp (wall-clock at produce time)
    # so the UI's "last 24 hours" filter aligns with demo time, matching the
    # ksqlDB tumbling-window behaviour for trending_movies.
    if genres:
        wall_clock_sec = kafka_ts_ms // 1000
        ws, we = _window_bucket(wall_clock_sec)
        for genre in (g.strip() for g in genres.split(",") if g.strip()):
            count, rating_sum = buffers.genre_counts[(ws, we, genre)]
            buffers.genre_counts[(ws, we, genre)] = (count + 1, rating_sum + rating)


def _handle_trending(buffers: Buffers, movies: dict, msg) -> None:
    value = msg.value
    if value is None:
        return
    try:
        # ksqlDB windowed tables put the GROUP BY column in the Kafka key
        # (8 bytes BIGINT big-endian), followed by an 8-byte window suffix.
        if msg.key and len(msg.key) >= 8:
            movie_id = int.from_bytes(msg.key[:8], byteorder="big", signed=True)
        else:
            mid = value.get("MOVIE_ID") or value.get("movie_id")
            if mid is None:
                return
            movie_id = int(mid)

        window_start = int(value.get("WINDOWSTART") or value.get("WINDOW_START")
                           or value.get("window_start") or 0)
        window_end = int(value.get("WINDOWEND") or value.get("WINDOW_END")
                         or value.get("window_end") or 0)
        rating_count = int(value.get("RATING_COUNT") or value.get("rating_count") or 0)
        avg_rating = value.get("AVG_RATING") or value.get("avg_rating")
    except (TypeError, ValueError) as exc:
        LOG.warning("skip malformed trending msg: %s | value=%s", exc, value)
        return

    if window_start <= 0 or rating_count <= 0:
        return

    title, genres = movies.get(movie_id, (None, None))
    buffers.trending[(window_start, movie_id)] = (
        _to_dt(window_start // 1000 if window_start > 1e12 else window_start),
        _to_dt(window_end // 1000 if window_end > 1e12 else window_end),
        movie_id,
        title,
        genres,
        rating_count,
        float(avg_rating) if avg_rating is not None else None,
    )


async def _run() -> None:
    if not NEON_DSN:
        raise SystemExit("NEON_DSN env var is required")
    movies = _load_movies()
    LOG.info("loaded %d movies for enrichment", len(movies))

    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = False
    buffers = Buffers()

    consumer = AIOKafkaConsumer(
        TOPIC_RATINGS,
        TOPIC_TRENDING,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    await consumer.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    LOG.info("subscribed to %s and %s", TOPIC_RATINGS, TOPIC_TRENDING)

    last_flush = time.monotonic()
    try:
        while not stop.is_set():
            batch = await consumer.getmany(timeout_ms=1000, max_records=1000)
            for tp, messages in batch.items():
                for msg in messages:
                    if tp.topic == TOPIC_RATINGS:
                        if msg.value:
                            _handle_rating(buffers, movies, msg.value, msg.timestamp)
                    elif tp.topic == TOPIC_TRENDING:
                        _handle_trending(buffers, movies, msg)
            if time.monotonic() - last_flush >= FLUSH_INTERVAL_SEC:
                await _flush(buffers, conn)
                await consumer.commit()
                last_flush = time.monotonic()
        LOG.info("stop signal received; flushing final batch")
        await _flush(buffers, conn)
        await consumer.commit()
    finally:
        await consumer.stop()
        conn.close()


def main() -> int:
    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

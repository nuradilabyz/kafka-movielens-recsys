"""Replay MovieLens ratings into Kafka, ordered by event timestamp.

Each tick of wall-clock time covers SPEED_FACTOR seconds of dataset time, so
SPEED_FACTOR=86400 replays one day of ratings per second.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from aiokafka import AIOKafkaProducer

LOG = logging.getLogger("producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

ROOT = Path(__file__).resolve().parent.parent
RATINGS_CSV = Path(os.getenv("RATINGS_CSV_PATH", ROOT / "data/ml-32m/ratings.csv"))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_RATINGS", "movielens.ratings.raw")
SPEED_FACTOR = float(os.getenv("SPEED_FACTOR", "86400"))
START_DATE = os.getenv("RATINGS_START_DATE")
END_DATE = os.getenv("RATINGS_END_DATE")
CHUNK_SIZE = int(os.getenv("PRODUCER_CHUNK_SIZE", "500000"))
MAX_EVENTS = int(os.getenv("PRODUCER_MAX_EVENTS", "0"))  # 0 = unlimited
LOG_EVERY = int(os.getenv("PRODUCER_LOG_EVERY", "5000"))


def _to_epoch(date_str: str | None) -> int | None:
    if not date_str:
        return None
    return int(datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc).timestamp())


def _load_filtered_sorted() -> pd.DataFrame:
    start_ts = _to_epoch(START_DATE)
    end_ts = _to_epoch(END_DATE)
    LOG.info("loading %s (chunks of %d rows)", RATINGS_CSV, CHUNK_SIZE)
    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(RATINGS_CSV, chunksize=CHUNK_SIZE):
        if start_ts is not None:
            chunk = chunk[chunk["timestamp"] >= start_ts]
        if end_ts is not None:
            chunk = chunk[chunk["timestamp"] < end_ts]
        if not chunk.empty:
            frames.append(chunk)
    if not frames:
        raise SystemExit("no rows match the configured date range")
    df = pd.concat(frames, ignore_index=True)
    df.sort_values("timestamp", inplace=True, kind="mergesort")
    df.reset_index(drop=True, inplace=True)
    LOG.info(
        "%d events loaded; ts range %s..%s",
        len(df),
        datetime.fromtimestamp(int(df["timestamp"].iloc[0]), tz=timezone.utc).isoformat(),
        datetime.fromtimestamp(int(df["timestamp"].iloc[-1]), tz=timezone.utc).isoformat(),
    )
    return df


async def _produce(df: pd.DataFrame) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=20,
        acks=1,
    )
    await producer.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        base_event_ts = int(df["timestamp"].iloc[0])
        base_wall = time.monotonic()
        sent = 0
        for row in df.itertuples(index=False):
            if stop.is_set():
                break

            event_ts = int(row.timestamp)
            dataset_elapsed = event_ts - base_event_ts
            target_wall_elapsed = dataset_elapsed / SPEED_FACTOR
            actual_wall_elapsed = time.monotonic() - base_wall
            sleep_for = target_wall_elapsed - actual_wall_elapsed
            if sleep_for > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=sleep_for)
                    break
                except asyncio.TimeoutError:
                    pass

            payload = {
                "user_id": int(row.userId),
                "movie_id": int(row.movieId),
                "rating": float(row.rating),
                "ts": event_ts,
            }
            await producer.send_and_wait(TOPIC, value=payload, key=row.userId)
            sent += 1
            if sent % LOG_EVERY == 0:
                rate = sent / max(time.monotonic() - base_wall, 1e-6)
                LOG.info("sent=%d rate=%.0f msg/s last_event_ts=%d", sent, rate, event_ts)
            if MAX_EVENTS and sent >= MAX_EVENTS:
                LOG.info("reached PRODUCER_MAX_EVENTS=%d, stopping", MAX_EVENTS)
                break
        LOG.info("producer loop done; total sent=%d", sent)
    finally:
        await producer.stop()


def main() -> int:
    if not RATINGS_CSV.exists():
        LOG.error("%s not found; run scripts/download_movielens.py first", RATINGS_CSV)
        return 1
    df = _load_filtered_sorted()
    asyncio.run(_produce(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

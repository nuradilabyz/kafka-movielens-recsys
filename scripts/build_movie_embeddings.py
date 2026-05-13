"""Encode movies into 384-dim vectors and upsert them into Qdrant.

Text used for embedding: "{title} | {genres} | {top_tags}".
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from sentence_transformers import SentenceTransformer

ROOT = Path(__file__).resolve().parent.parent
MOVIES_CSV = Path(os.getenv("MOVIES_CSV_PATH", ROOT / "data/ml-32m/movies.csv"))
TAGS_CSV = Path(os.getenv("TAGS_CSV_PATH", ROOT / "data/ml-32m/tags.csv"))
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "movies")
MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "384"))
BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "256"))
TOP_TAGS_PER_MOVIE = 5


def _build_corpus() -> pd.DataFrame:
    movies = pd.read_csv(MOVIES_CSV)
    movies["genres"] = movies["genres"].fillna("").str.replace("|", ", ", regex=False)

    if TAGS_CSV.exists():
        tags = pd.read_csv(TAGS_CSV, usecols=["movieId", "tag"])
        tags["tag"] = tags["tag"].astype(str).str.lower().str.strip()
        top_tags = (
            tags.groupby(["movieId", "tag"]).size().reset_index(name="n")
            .sort_values(["movieId", "n"], ascending=[True, False])
            .groupby("movieId")
            .head(TOP_TAGS_PER_MOVIE)
            .groupby("movieId")["tag"]
            .apply(lambda s: ", ".join(s))
            .reset_index()
            .rename(columns={"tag": "top_tags"})
        )
        movies = movies.merge(top_tags, on="movieId", how="left")
    else:
        movies["top_tags"] = ""

    movies["top_tags"] = movies["top_tags"].fillna("")
    movies["doc"] = (
        movies["title"].fillna("")
        + " | "
        + movies["genres"]
        + " | "
        + movies["top_tags"]
    )
    return movies[["movieId", "title", "genres", "doc"]]


def _ensure_collection(client: QdrantClient) -> None:
    existing = {c.name for c in client.get_collections().collections}
    if COLLECTION in existing:
        print(f"collection '{COLLECTION}' already exists; will upsert into it")
        return
    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=qmodels.VectorParams(
            size=EMBEDDING_DIM, distance=qmodels.Distance.COSINE
        ),
    )
    print(f"created collection '{COLLECTION}' (dim={EMBEDDING_DIM}, cosine)")


def main() -> int:
    if not MOVIES_CSV.exists():
        print(f"ERROR: {MOVIES_CSV} not found. Run scripts/download_movielens.py first.", file=sys.stderr)
        return 1

    corpus = _build_corpus()
    print(f"loaded {len(corpus):,} movies")

    model = SentenceTransformer(MODEL_NAME)
    client = QdrantClient(url=QDRANT_URL)
    _ensure_collection(client)

    total = len(corpus)
    for start in range(0, total, BATCH_SIZE):
        chunk = corpus.iloc[start : start + BATCH_SIZE]
        vectors = model.encode(
            chunk["doc"].tolist(),
            batch_size=BATCH_SIZE,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        points = [
            qmodels.PointStruct(
                id=int(row.movieId),
                vector=vec.tolist(),
                payload={
                    "movie_id": int(row.movieId),
                    "title": row.title,
                    "genres": row.genres,
                },
            )
            for row, vec in zip(chunk.itertuples(index=False), vectors)
        ]
        client.upsert(collection_name=COLLECTION, points=points)
        done = min(start + BATCH_SIZE, total)
        print(f"  upserted {done:,}/{total:,}")

    info = client.get_collection(COLLECTION)
    print(f"done. collection points: {info.points_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

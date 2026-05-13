"""Project all movie vectors in Qdrant to 2D via UMAP for the taste-map UI.

Outputs into ui/:
  - movie_map.parquet           – per-movie (movie_id, x, y, title, primary_genre)
  - movie_vectors_384d.npy      – raw 384-dim vectors (float32) aligned with…
  - movie_ids.npy               – …this array of movie_ids (int64)

We don't save a fitted UMAP model: projecting a new vector via UMAP.transform()
is fragile and slow. Instead, taste_map.py projects new vectors using kNN —
find K nearest movies in the original 384-d space and average their 2-d
coordinates. That is semantically the same statement as "the user is near
these similar movies", and produces stable, fast results.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import umap
from qdrant_client import QdrantClient

ROOT = Path(__file__).resolve().parent.parent
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "movies")
SCROLL_BATCH = int(os.getenv("MAP_SCROLL_BATCH", "1000"))
OUT_DIR = ROOT / "ui"


def _fetch_all_movies(client: QdrantClient) -> tuple[np.ndarray, list[dict]]:
    ids: list[int] = []
    payloads: list[dict] = []
    vectors: list[list[float]] = []
    offset = None
    while True:
        points, offset = client.scroll(
            collection_name=COLLECTION,
            limit=SCROLL_BATCH,
            offset=offset,
            with_vectors=True,
            with_payload=True,
        )
        if not points:
            break
        for p in points:
            if p.vector is None:
                continue
            ids.append(int(p.id))
            payloads.append(p.payload or {})
            vectors.append(p.vector)
        if offset is None:
            break
    arr = np.asarray(vectors, dtype=np.float32)
    print(f"fetched {len(ids):,} movies from Qdrant, vectors shape: {arr.shape}")
    return arr, [{"id": i, **p} for i, p in zip(ids, payloads)]


def _primary_genre(genres: str | None) -> str:
    if not genres:
        return "Unknown"
    head = genres.split(",", 1)[0].strip()
    return head or "Unknown"


def main() -> int:
    client = QdrantClient(url=QDRANT_URL)
    vectors, meta = _fetch_all_movies(client)
    if vectors.size == 0:
        print("ERROR: no vectors in Qdrant — run build_movie_embeddings.py first", file=sys.stderr)
        return 1

    print("fitting UMAP (this is the slow part, ~1–2 min)...")
    reducer = umap.UMAP(
        n_components=2,
        n_neighbors=15,
        min_dist=0.1,
        metric="cosine",
        random_state=42,
        verbose=True,
    )
    coords = reducer.fit_transform(vectors)
    print(f"UMAP done, coords shape: {coords.shape}")

    rows = [
        {
            "movie_id": m["id"],
            "title": m.get("title") or f"#{m['id']}",
            "primary_genre": _primary_genre(m.get("genres")),
            "genres": m.get("genres") or "",
            "x": float(x),
            "y": float(y),
        }
        for m, (x, y) in zip(meta, coords)
    ]
    df = pd.DataFrame(rows)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    map_path = OUT_DIR / "movie_map.parquet"
    df.to_parquet(map_path, index=False)
    np.save(OUT_DIR / "movie_vectors_384d.npy", vectors)
    np.save(OUT_DIR / "movie_ids.npy", np.array([m["id"] for m in meta], dtype=np.int64))
    print(f"saved {map_path} ({len(df):,} rows)")
    print(f"saved {OUT_DIR / 'movie_vectors_384d.npy'} (shape {vectors.shape})")
    print(f"saved {OUT_DIR / 'movie_ids.npy'} (shape {vectors.shape[0]})")

    # Clean up old PCA artefacts if present.
    for stale in ("pca_components.npy", "pca_mean.npy"):
        p = OUT_DIR / stale
        if p.exists():
            p.unlink()
            print(f"removed stale {p.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

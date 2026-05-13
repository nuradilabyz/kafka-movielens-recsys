"""Helpers for the 'Карта вкусов' visualization.

Movies are projected to 2D once by UMAP (offline, scripts/build_movie_map_2d.py).
A new vector (e.g. user's EMA vector) is projected on the fly with kNN:
we find K nearest movies in the original 384-d space (cosine distance), then
return a distance-weighted centroid of their pre-computed 2D coordinates.
This avoids fitting/loading a UMAP model and gives stable, fast projections.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from qdrant_client import QdrantClient

UI_DIR = Path(__file__).resolve().parent
MAP_PATH = UI_DIR / "movie_map.parquet"
VECTORS_PATH = UI_DIR / "movie_vectors_384d.npy"
IDS_PATH = UI_DIR / "movie_ids.npy"

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "movies")
ALPHA = float(os.getenv("USER_VEC_EMA_ALPHA", "0.85"))
KNN_K = int(os.getenv("TASTE_MAP_KNN_K", "15"))


@st.cache_resource(show_spinner=False)
def load_movie_map() -> pd.DataFrame | None:
    if not MAP_PATH.exists():
        return None
    return pd.read_parquet(MAP_PATH)


@st.cache_resource(show_spinner=False)
def _load_projection_data() -> tuple[np.ndarray, np.ndarray, np.ndarray] | None:
    """Returns (movie_vectors_384d, movie_ids, movie_xy_coords) aligned by row."""
    if not (VECTORS_PATH.exists() and IDS_PATH.exists() and MAP_PATH.exists()):
        return None
    vectors = np.load(VECTORS_PATH).astype(np.float32)
    ids = np.load(IDS_PATH).astype(np.int64)
    df = pd.read_parquet(MAP_PATH).set_index("movie_id")
    xy = df.loc[ids, ["x", "y"]].to_numpy(dtype=np.float32)
    # Normalize so cosine similarity == dot product.
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return vectors / norms, ids, xy


@st.cache_resource(show_spinner=False)
def _qdrant() -> QdrantClient | None:
    """Returns Qdrant client if reachable, else None (e.g. cloud deploy)."""
    try:
        c = QdrantClient(url=QDRANT_URL, timeout=2.0)
        c.get_collections()  # probe
        return c
    except Exception:
        return None


def project(vec: np.ndarray) -> tuple[float, float]:
    """Project a 384-d vector to the 2D taste map via kNN-weighted centroid."""
    loaded = _load_projection_data()
    if loaded is None:
        return 0.0, 0.0
    movie_vecs, _ids, movie_xy = loaded

    v = vec.astype(np.float32)
    n = float(np.linalg.norm(v))
    if n == 0:
        return 0.0, 0.0
    v = v / n

    sims = movie_vecs @ v  # cosine similarity, shape (N,)
    k = min(KNN_K, sims.shape[0])
    top_idx = np.argpartition(-sims, k - 1)[:k]
    # Weight by softmax over similarities so the centroid leans toward the
    # closest neighbours.
    s = sims[top_idx]
    s = s - s.max()
    w = np.exp(s)
    w = w / w.sum()
    xy = movie_xy[top_idx]
    centroid = (w[:, None] * xy).sum(axis=0)
    return float(centroid[0]), float(centroid[1])


def qdrant_available() -> bool:
    return _qdrant() is not None


def fetch_movie_vectors(movie_ids: list[int]) -> dict[int, np.ndarray]:
    if not movie_ids:
        return {}
    client = _qdrant()
    if client is None:
        return {}
    try:
        points = client.retrieve(
            collection_name=COLLECTION,
            ids=movie_ids,
            with_vectors=True,
            with_payload=False,
        )
    except Exception:
        return {}
    return {
        int(p.id): np.asarray(p.vector, dtype=np.float32)
        for p in points
        if p.vector is not None
    }


def simulate_user_trail(rated_movie_ids_chronological: list[int]) -> list[tuple[float, float]]:
    """Replay the EMA exactly like the recommender, return 2D trail."""
    if not rated_movie_ids_chronological:
        return []

    vectors = fetch_movie_vectors(rated_movie_ids_chronological)
    if not vectors:
        return []

    user_vec: np.ndarray | None = None
    trail: list[tuple[float, float]] = []
    for movie_id in rated_movie_ids_chronological:
        v = vectors.get(movie_id)
        if v is None:
            continue
        if user_vec is None:
            user_vec = v.copy()
        else:
            user_vec = ALPHA * user_vec + (1.0 - ALPHA) * v
        norm = float(np.linalg.norm(user_vec))
        if norm > 0:
            user_vec = user_vec / norm
        trail.append(project(user_vec))
    return trail

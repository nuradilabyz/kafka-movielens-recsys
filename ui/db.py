"""Thin Neon Postgres helper for Streamlit pages."""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st


def _dsn() -> str:
    dsn = os.getenv("NEON_DSN")
    if not dsn:
        try:
            dsn = st.secrets["NEON_DSN"]
        except (FileNotFoundError, KeyError, Exception):
            dsn = None
    if not dsn:
        st.error("NEON_DSN is not configured (env var or Streamlit secrets).")
        st.stop()
    return dsn


@st.cache_resource(show_spinner=False)
def _conn():
    return psycopg2.connect(_dsn())


def query(sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
    conn = _conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return list(cur.fetchall())
    except psycopg2.OperationalError:
        _conn.clear()
        raise


def query_df(sql: str, params: tuple | None = None) -> pd.DataFrame:
    return pd.DataFrame(query(sql, params))


def get_pipeline_metrics() -> list[dict[str, Any]]:
    return query(
        "SELECT component, messages_total, last_event_ts, updated_at "
        "FROM pipeline_metrics ORDER BY component"
    )

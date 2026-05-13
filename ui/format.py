"""Shared formatting helpers for the Russian UI."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

MONTHS_RU = [
    "янв", "фев", "мар", "апр", "май", "июн",
    "июл", "авг", "сен", "окт", "ноя", "дек",
]


def fmt_dt(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if not isinstance(value, datetime):
        return str(value)
    return f"{value.day:02d} {MONTHS_RU[value.month - 1]} {value.year}, {value.strftime('%H:%M')}"


def fmt_dt_short(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if not isinstance(value, datetime):
        return str(value)
    return value.strftime("%H:%M:%S")


def humanize_age(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if not isinstance(value, datetime):
        return str(value)
    now = datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = now - value
    secs = int(delta.total_seconds())
    if secs < 0:
        return "только что"
    if secs < 60:
        return f"{secs} сек назад"
    if secs < 3600:
        return f"{secs // 60} мин назад"
    if secs < 86400:
        return f"{secs // 3600} ч назад"
    return f"{secs // 86400} дн назад"


def format_dataframe_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(fmt_dt)
    return df

"""Состояние pipeline: метрики, лента событий, история окон."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query_df  # noqa: E402
from format import fmt_dt, humanize_age  # noqa: E402

REFRESH_SEC = int(os.getenv("STREAMLIT_REFRESH_SEC", "5"))

st.set_page_config(page_title="Состояние", page_icon="🛰️", layout="wide")
st_autorefresh(interval=REFRESH_SEC * 1000, key="pipeline_refresh")

st.title("🛰️ Состояние pipeline")
st.caption(f"Авто-обновление каждые {REFRESH_SEC} секунд")

st.info(
    "**Зачем эта страница.** Тут видно, **живы ли консьюмеры**. "
    "Грубо: если `обновлено: X секунд назад` — pipeline активен, "
    "данные льются. Если `5 минут назад / 1 час назад` — продьюсер "
    "выключен или консьюмер упал.\n\n"
    "**Кто такие консьюмеры:**\n"
    "- **Аналитика** — читает каждое событие из Kafka, обогащает названием "
    "  и жанром, пишет в `events_enriched`. Cчётчик растёт `+1 на каждое "
    "  событие`.\n"
    "- **Рекомендатор** — обновляет «вкус» юзера и кладёт ему top-10 "
    "  фильмов из Qdrant. Cчётчик растёт `+1 на каждый уникальный UPSERT "
    "  в user_recs`. Поэтому он **сильно меньше**: за 5 секунд один и тот "
    "  же юзер мог получить 3 оценки — в счётчик пойдёт +1, не +3. "
    "  Плюс события для фильмов вне Qdrant (~84%) полностью пропускаются.",
    icon="ℹ️",
)

st.subheader("📊 Метрики консьюмеров")
metrics_df = query_df(
    "SELECT component, messages_total, last_event_ts, updated_at "
    "FROM pipeline_metrics ORDER BY component"
)
if metrics_df.empty:
    st.warning("Метрик пока нет.")
else:
    labels_ru = {
        "recommender": "Рекомендатор",
        "analytics_sink": "Аналитика",
    }
    counter_labels = {
        "recommender": "UPSERT-ов в user_recs",
        "analytics_sink": "Событий обработано",
    }
    metrics_df["Компонент"] = metrics_df["component"].map(labels_ru).fillna(metrics_df["component"])
    metrics_df["Что считается"] = metrics_df["component"].map(counter_labels).fillna("—")
    metrics_df["Сколько"] = metrics_df["messages_total"].apply(
        lambda x: f"{x:,}".replace(",", " ")
    )
    metrics_df["Последнее событие"] = metrics_df["last_event_ts"].apply(fmt_dt)
    metrics_df["Обновлено"] = metrics_df["updated_at"].apply(humanize_age)
    st.dataframe(
        metrics_df[["Компонент", "Что считается", "Сколько", "Последнее событие", "Обновлено"]],
        hide_index=True,
        use_container_width=True,
    )

st.divider()

st.subheader("📡 Последние события из Kafka")
st.caption(
    "Лента того, что прямо сейчас проходит через pipeline. "
    "Колонка `Получено` — когда событие попало в Postgres (реальное время). "
    "Колонка `Дата события` — оригинальный timestamp из MovieLens (2019)."
)

events = query_df(
    """
    SELECT ingested_at, event_ts, user_id, movie_id, title, genres, rating
    FROM events_enriched
    ORDER BY id DESC
    LIMIT 100
    """
)
if events.empty:
    st.info("Событий ещё не было.")
else:
    events["Получено"] = events["ingested_at"].apply(humanize_age)
    events["Дата события"] = events["event_ts"].apply(fmt_dt)
    events = events.rename(columns={
        "user_id": "Юзер",
        "movie_id": "ID фильма",
        "title": "Название",
        "genres": "Жанры",
        "rating": "Оценка",
    })
    st.dataframe(
        events[["Получено", "Дата события", "Юзер", "ID фильма", "Название", "Жанры", "Оценка"]],
        hide_index=True,
        use_container_width=True,
    )

st.divider()

st.subheader("🪟 История окон трендов")
st.caption(
    "Каждая строка — отдельное 5-минутное окно агрегата ksqlDB. "
    "Видно, как pipeline «дышал» во времени: больше уникальных фильмов "
    "и оценок = активный поток событий."
)

windows = query_df(
    """
    SELECT window_start,
           COUNT(*) AS movies_in_window,
           SUM(rating_count) AS total_ratings
    FROM trending_movies
    GROUP BY window_start
    ORDER BY window_start DESC
    LIMIT 24
    """
)
if windows.empty:
    st.info("Окон ещё нет.")
else:
    windows["Начало окна"] = windows["window_start"].apply(fmt_dt)
    windows["Возраст"] = windows["window_start"].apply(humanize_age)
    windows = windows.rename(columns={
        "movies_in_window": "Уникальных фильмов",
        "total_ratings": "Всего оценок",
    })
    st.dataframe(
        windows[["Начало окна", "Возраст", "Уникальных фильмов", "Всего оценок"]],
        hide_index=True,
        use_container_width=True,
    )

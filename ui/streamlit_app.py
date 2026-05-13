"""Главная страница: обзор pipeline и быстрые метрики."""

from __future__ import annotations

import streamlit as st

from db import query
from format import humanize_age

st.set_page_config(
    page_title="MovieLens Realtime",
    page_icon="🎬",
    layout="wide",
)

st.title("🎬 MovieLens — рекомендации в реальном времени")
st.caption(
    "Потоковый pipeline: продьюсер → Kafka → ksqlDB + два Python-консьюмера → "
    "Neon Postgres → этот UI. Данные обновляются каждые 5 секунд."
)

st.info(
    "**Что тут происходит?** Каждую секунду продьюсер заливает в Kafka "
    "историческое событие из MovieLens 32M (≈32 млн оценок). Один консьюмер "
    "считает «трендовые» фильмы за последние 5 минут, второй обновляет "
    "вектор предпочтений каждого пользователя и кладёт ему top-10 похожих "
    "фильмов из Qdrant. Всё это попадает в Postgres и сюда — на 3 страницы слева.",
    icon="ℹ️",
)

st.subheader("📊 Состояние pipeline")

metrics = query(
    "SELECT component, messages_total, last_event_ts, updated_at "
    "FROM pipeline_metrics ORDER BY component"
)
if not metrics:
    st.warning(
        "Метрики пока пусты. Запусти локальный стэк и продьюсера, "
        "чтобы наполнить Neon."
    )
else:
    labels_ru = {
        "recommender": "Рекомендатор",
        "analytics_sink": "Аналитика",
    }
    suffix_ru = {
        "recommender": "обновлений вектора",
        "analytics_sink": "событий обработано",
    }
    cols = st.columns(len(metrics))
    for col, row in zip(cols, metrics):
        name = labels_ru.get(row["component"], row["component"])
        suffix = suffix_ru.get(row["component"], "")
        with col:
            value = f"{row['messages_total']:,}".replace(",", " ")
            st.metric(
                label=name,
                value=f"{value} {suffix}".strip(),
                delta=humanize_age(row["updated_at"]),
                delta_color="off",
            )

st.divider()

st.markdown(
    """
    ### 📑 Страницы

    **🔥 Тренды** — топ-фильмы за последнее 5-минутное окно. Считает ksqlDB
    прямо в стриме (это streaming SQL поверх Kafka). Ниже на той же странице
    — бар-чарт топ-жанров за сутки.

    **✨ Рекомендации пользователя** — выбери любого недавнего юзера и увидь
    его top-10 фильмов. Под капотом — Qdrant векторный поиск: каждый рейтинг
    обновляет «вкусовой вектор», система ищет 10 ближайших в 384-мерном
    пространстве.

    **🛰️ Pipeline** — техническое состояние: сколько событий обработано,
    что прямо сейчас прилетает, история окон. Полезно если что-то пошло не так.
    """
)

st.divider()

with st.expander("🛠 Что под капотом (стэк)"):
    st.markdown(
        """
        | Слой | Инструмент |
        | --- | --- |
        | Данные | MovieLens 32M (`ratings.csv`, `movies.csv`, `tags.csv`) |
        | Брокер | Kafka 7.6 в KRaft-режиме |
        | Streaming SQL | ksqlDB (агрегаты за окна) |
        | Векторный поиск | Qdrant 1.11 |
        | Продьюсер/консьюмеры | Python + `aiokafka` |
        | База-мост | Neon Postgres (free tier) |
        | UI | Streamlit Community Cloud |
        """
    )

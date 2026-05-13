"""Рекомендации для пользователя: top-N из Qdrant + карта вкусов."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query, query_df  # noqa: E402
from format import fmt_dt, humanize_age  # noqa: E402
import numpy as np  # noqa: E402

from taste_map import load_movie_map, project, simulate_user_trail  # noqa: E402

REFRESH_SEC = int(os.getenv("STREAMLIT_REFRESH_SEC", "5"))

st.set_page_config(page_title="Рекомендации", page_icon="✨", layout="wide")
st_autorefresh(interval=REFRESH_SEC * 1000, key="recs_refresh")

st.title("✨ Что посоветовать пользователю")
st.caption(f"Авто-обновление каждые {REFRESH_SEC} секунд")

st.info(
    "**Что тут.** Каждый пользователь в датасете MovieLens 32M оставлял "
    "оценки на фильмы. Мы превращаем каждый фильм в вектор (математическое "
    "представление сюжета/жанра — 384 числа). Когда юзер ставит оценку, мы "
    "двигаем его «вкусовой вектор» в сторону этого фильма. Затем берём 10 "
    "фильмов, чьи векторы ближе всего к вкусовому вектору юзера — это и "
    "есть **рекомендации для следующего просмотра**.\n\n"
    "**Колонка `Похожесть`** — насколько близок вектор фильма к вектору "
    "юзера (1.0 = идеально, 0.0 = совсем не похож).\n\n"
    "**Зачем «Последние оценки этого юзера»** — это входные данные, по которым "
    "система построила его вектор. Если он смотрел драмы — рекомендации "
    "будут драмами.",
    icon="ℹ️",
)

recent_users = query(
    "SELECT user_id, updated_at FROM user_recs ORDER BY updated_at DESC LIMIT 200"
)
if not recent_users:
    st.warning("Рекомендаций пока нет — запусти рекомендатор.")
    st.stop()

user_options = [row["user_id"] for row in recent_users]
ts_by_user = {row["user_id"]: row["updated_at"] for row in recent_users}

# Sticky-выбор пользователя: запоминаем в session_state, чтобы auto-refresh
# не сбрасывал на самого свежего. Если ранее выбранный юзер вылетел из топ-200 —
# добавляем его в список искусственно, чтобы выбор сохранился.
if "selected_user_id" not in st.session_state:
    st.session_state.selected_user_id = user_options[0]

pinned = st.session_state.selected_user_id
if pinned not in user_options:
    user_options = [pinned] + user_options
    ts_by_user.setdefault(pinned, None)

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    user_id = st.selectbox(
        "Выбери пользователя (выбор сохранится между обновлениями)",
        options=user_options,
        index=user_options.index(pinned),
        format_func=lambda u: f"Юзер {u}"
            + (f" · {humanize_age(ts_by_user[u])}" if ts_by_user.get(u) else ""),
        key="user_selector",
    )
with col2:
    manual = st.text_input("Или ID вручную", "")
with col3:
    st.write("")
    if st.button("⏭ Взять самого свежего", use_container_width=True):
        user_id = recent_users[0]["user_id"]

if manual.strip().isdigit():
    user_id = int(manual.strip())

st.session_state.selected_user_id = user_id

row = query(
    "SELECT user_id, recs, user_vec, updated_at FROM user_recs WHERE user_id = %s",
    (user_id,),
)
if not row:
    st.warning(f"Для юзера {user_id} рекомендаций ещё нет.")
    st.stop()

recs = row[0]["recs"]
if isinstance(recs, str):
    recs = json.loads(recs)
user_vec_saved = row[0].get("user_vec")
updated_at = row[0]["updated_at"]

st.metric(
    "Вектор юзера обновлялся",
    fmt_dt(updated_at),
    humanize_age(updated_at),
    delta_color="off",
)

st.subheader(f"🎯 Что посоветуем юзеру {user_id} (топ-{len(recs)})")
st.caption("Это фильмы, которые ему стоит посмотреть следующими — по нашим оценкам.")

recs_df = pd.DataFrame(recs)
recs_df.insert(0, "Топ", [f"#{i + 1}" for i in range(len(recs_df))])
recs_df = recs_df.rename(columns={
    "movie_id": "ID",
    "title": "Название",
    "genres": "Жанры",
    "score": "Похожесть",
})
if "Похожесть" in recs_df.columns:
    recs_df["Похожесть"] = recs_df["Похожесть"].round(3)
st.dataframe(recs_df, hide_index=True, use_container_width=True)

st.divider()

# ───────────────────────── КАРТА ВКУСОВ ─────────────────────────

st.subheader("🗺️ Карта вкусов")
st.caption(
    "Каждая точка — фильм, спроецированный из 384-мерного пространства "
    "в 2D через PCA. Похожие фильмы стоят рядом. **Звезда** — текущая "
    "позиция юзера. **Пунктирная линия** — его «след»: куда он сдвигался "
    "после каждой следующей оценки."
)

movie_map = load_movie_map()
if movie_map is None:
    st.warning(
        "Карта ещё не построена. Запусти `python scripts/build_movie_map_2d.py`."
    )
else:
    events_chronological = query_df(
        """
        SELECT movie_id, title, genres, rating, event_ts
        FROM events_enriched
        WHERE user_id = %s
        ORDER BY id ASC
        """,
        (user_id,),
    )

    if events_chronological.empty:
        st.info(
            "События этого юзера уже вытеснились из хвостового буфера "
            "(`events_enriched` хранит только последние ~5000 событий). "
            "Трейл и синие точки нарисовать неоткуда, но **звезда всё "
            "равно стоит** — мы спроецировали сохранённый `user_vec` "
            "из таблицы `user_recs`."
        )
        rated_ids: list[int] = []
        trail: list[tuple[float, float]] = []
        if user_vec_saved is not None:
            saved_arr = np.asarray(user_vec_saved, dtype=np.float32)
            trail = [project(saved_arr)]
    else:
        rated_ids = events_chronological["movie_id"].astype(int).tolist()
        trail = simulate_user_trail(rated_ids)
        # Always pin the star to the saved vector if available — more accurate
        # than reconstructing the trail (events_enriched may have lost some).
        if user_vec_saved is not None:
            saved_arr = np.asarray(user_vec_saved, dtype=np.float32)
            trail.append(project(saved_arr))

    map_col1, map_col2 = st.columns([1, 1])
    with map_col1:
        show_background = st.checkbox(
            "Показать фон (все фильмы)",
            value=True,
            help="Уберёт серое облако точек — останется только траектория и оценки юзера.",
        )
    with map_col2:
        color_by_genre = st.checkbox(
            "Раскрасить фон по жанрам",
            value=False,
            help="Иначе все фоновые точки серые, что чище визуально.",
        )

    sample = movie_map
    if len(sample) > 2500:
        sample = sample.sample(2500, random_state=42)

    rated_set = set(rated_ids)
    sample_rated = movie_map[movie_map["movie_id"].isin(rated_set)]

    fig = go.Figure()

    if show_background:
        if color_by_genre:
            palette = {
                "Action": "#e41a1c", "Adventure": "#ff7f00", "Animation": "#984ea3",
                "Children": "#f781bf", "Comedy": "#a65628", "Crime": "#4daf4a",
                "Documentary": "#999999", "Drama": "#377eb8", "Fantasy": "#ffff33",
                "Film-Noir": "#1b9e77", "Horror": "#7570b3", "Mystery": "#e7298a",
                "Romance": "#66a61e", "Sci-Fi": "#e6ab02", "Thriller": "#a6761d",
                "War": "#666666", "Western": "#b15928", "Musical": "#ff69b4",
                "IMAX": "#8dd3c7", "Unknown": "#cccccc",
            }
            top_genres = sample["primary_genre"].value_counts().head(10).index.tolist()
            for genre in top_genres:
                sub = sample[sample["primary_genre"] == genre]
                fig.add_trace(
                    go.Scattergl(
                        x=sub["x"],
                        y=sub["y"],
                        mode="markers",
                        marker=dict(
                            size=3,
                            color=palette.get(genre, "#cccccc"),
                            opacity=0.25,
                        ),
                        hovertext=sub["title"] + " · " + sub["primary_genre"],
                        hoverinfo="text",
                        name=genre,
                        legendgroup="bg",
                        legendgrouptitle_text="Фильмы (фон)",
                    )
                )
        else:
            fig.add_trace(
                go.Scattergl(
                    x=sample["x"],
                    y=sample["y"],
                    mode="markers",
                    marker=dict(size=3, color="#d0d0d0", opacity=0.4),
                    hovertext=sample["title"] + " · " + sample["primary_genre"],
                    hoverinfo="text",
                    name="Все фильмы",
                    showlegend=True,
                )
            )

    if not sample_rated.empty:
        fig.add_trace(
            go.Scattergl(
                x=sample_rated["x"],
                y=sample_rated["y"],
                mode="markers",
                marker=dict(size=9, color="#1f77b4", opacity=0.9, symbol="circle",
                            line=dict(color="white", width=1)),
                hovertext=sample_rated["title"] + " · " + sample_rated["primary_genre"],
                hoverinfo="text",
                name="Что юзер оценил",
            )
        )

    if trail:
        xs = [p[0] for p in trail]
        ys = [p[1] for p in trail]
        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys,
                mode="lines+markers",
                line=dict(color="#ff7f0e", width=3, dash="dot"),
                marker=dict(size=7, color="#ff7f0e", opacity=0.8,
                            line=dict(color="white", width=1)),
                name="След вектора (траектория)",
                hovertext=[f"шаг {i + 1} из {len(trail)}" for i in range(len(trail))],
                hoverinfo="text",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[xs[-1]],
                y=[ys[-1]],
                mode="markers",
                marker=dict(size=28, color="#d62728", symbol="star",
                            line=dict(color="white", width=3)),
                name=f"Юзер {user_id} (сейчас)",
                hovertext=[f"Юзер {user_id} · после {len(trail)} оценок"],
                hoverinfo="text",
            )
        )

    fig.update_layout(
        height=600,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis=dict(title="PCA-1", zeroline=False),
        yaxis=dict(title="PCA-2", zeroline=False),
        plot_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.caption(
        "🧭 **Как читать карту:**\n"
        "- серые точки — все 14K фильмов в 2D-проекции\n"
        "- синие — те, которые юзер оценил (вход для вектора)\n"
        "- оранжевый пунктир — траектория его вкуса после каждой оценки\n"
        "- красная звезда — где он сейчас\n\n"
        "Если оценок было много из одного жанра — звезда будет внутри "
        "плотного облака этого жанра."
    )

st.divider()

st.subheader("📜 Что юзер оценивал — на основе этого строился его вкус")
st.caption(
    "Это вход для рекомендаций выше: каждая оценка двигала вектор юзера. "
    "Если он смотрел фантастику — увидишь фантастику и в советах."
)

events = query_df(
    """
    SELECT event_ts, movie_id, title, genres, rating
    FROM events_enriched
    WHERE user_id = %s
    ORDER BY id DESC
    LIMIT 50
    """,
    (user_id,),
)
if events.empty:
    st.info("Событий от этого юзера в хвостовом буфере нет.")
else:
    events["event_ts"] = events["event_ts"].apply(fmt_dt)
    events = events.rename(columns={
        "event_ts": "Когда оценил",
        "movie_id": "ID",
        "title": "Название",
        "genres": "Жанры",
        "rating": "Оценка",
    })
    st.dataframe(events, hide_index=True, use_container_width=True)

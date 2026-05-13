"""Тренды: топ-фильмы за окно реального времени + общий топ."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query_df  # noqa: E402
from format import MONTHS_RU  # noqa: E402

REFRESH_SEC = int(os.getenv("STREAMLIT_REFRESH_SEC", "5"))
WINDOW_OPTIONS = [5, 15, 30, 60]  # minutes

st.set_page_config(page_title="Тренды", page_icon="🔥", layout="wide")
st_autorefresh(interval=REFRESH_SEC * 1000, key="trending_refresh")

st.title("🔥 Тренды")
st.caption(f"Авто-обновление каждые {REFRESH_SEC} секунд")

st.info(
    "**Как это работает.** Время режется на окна по 5 минут (минимальный "
    "размер). Пока окно открыто, ksqlDB пересчитывает счётчики каждый раз "
    "когда приходит новое событие — **те же строки в таблице, но число "
    "`Оценок` растёт у популярных фильмов, и порядок может меняться**. "
    "Когда 5 минут проходят, окно закрывается, начинается новое.\n\n"
    "**Размер окна можно увеличить** селектором ниже (15 / 30 / 60 минут) — "
    "тогда покажется агрегат по нескольким 5-минутным окнам. "
    "Это всё про *streaming-вопрос «что популярно сейчас»*. Полный «топ за "
    "всё время» — отдельный блок ниже.",
    icon="ℹ️",
)


def _fmt_window_label(dt: datetime) -> str:
    return (
        f"{dt.day:02d} {MONTHS_RU[dt.month - 1]} {dt.year}, "
        f"{dt.strftime('%H:%M')} UTC"
    )


latest_window_df = query_df(
    "SELECT MAX(window_start) AS window_start FROM trending_movies"
)
if latest_window_df.empty or latest_window_df.iloc[0]["window_start"] is None:
    st.warning("Данных по трендам пока нет. Запусти продьюсер.")
    st.stop()

window_start = latest_window_df.iloc[0]["window_start"]
if window_start.tzinfo is None:
    window_start = window_start.replace(tzinfo=timezone.utc)
window_end = window_start + timedelta(minutes=5)
now = datetime.now(timezone.utc)
is_active = now < window_end
time_left = window_end - now

window_size = st.radio(
    "Размер окна",
    options=WINDOW_OPTIONS,
    format_func=lambda x: f"{x} минут",
    horizontal=True,
    help=(
        "5 мин — одно последнее окно из ksqlDB. "
        "15 / 30 / 60 — суммируется по нескольким окнам."
    ),
)

aggregate_start = window_end - timedelta(minutes=window_size)

col1, col2, col3 = st.columns([3, 2, 2])
with col1:
    st.metric(
        "Окно (UTC)",
        f"{_fmt_window_label(aggregate_start)} — {window_end.strftime('%H:%M')}",
    )
with col2:
    if is_active and window_size == 5:
        mins, secs = divmod(int(time_left.total_seconds()), 60)
        st.metric("До нового окна", f"{mins} мин {secs:02d} сек")
    else:
        st.metric("Длительность окна", f"{window_size} минут")
with col3:
    top_n = st.slider("Сколько топ-фильмов показать", 5, 50, 20, 5)

if window_size == 5 and is_active:
    st.success(
        f"🟢 Окно активно — цифры обновляются по мере прихода событий. "
        f"До нового окна: {int(time_left.total_seconds() // 60)} мин "
        f"{int(time_left.total_seconds() % 60):02d} сек.",
        icon="🟢",
    )

if window_size == 5:
    df = query_df(
        """
        SELECT movie_id, title, genres, rating_count,
               ROUND(avg_rating::numeric, 2) AS avg_rating
        FROM trending_movies
        WHERE window_start = %s
        ORDER BY rating_count DESC
        LIMIT %s
        """,
        (window_start, top_n),
    )
else:
    df = query_df(
        """
        SELECT movie_id,
               MAX(title)  AS title,
               MAX(genres) AS genres,
               SUM(rating_count) AS rating_count,
               ROUND((SUM(avg_rating * rating_count) / NULLIF(SUM(rating_count), 0))::numeric, 2) AS avg_rating
        FROM trending_movies
        WHERE window_start >= %s
        GROUP BY movie_id
        ORDER BY rating_count DESC
        LIMIT %s
        """,
        (aggregate_start, top_n),
    )

df.insert(0, "Топ", [f"#{i + 1}" for i in range(len(df))])
df = df.rename(columns={
    "movie_id": "ID",
    "title": "Название",
    "genres": "Жанры",
    "rating_count": "Оценок",
    "avg_rating": "Рейтинг",
})
st.dataframe(df, hide_index=True, use_container_width=True)

if window_size == 5:
    st.caption(
        "Каждые 5 секунд страница перезапрашивает данные. Пока окно активно — "
        "увидишь, как `Оценок` растёт и порядок строк меняется."
    )
else:
    st.caption(
        f"Это суммированный топ за последние {window_size} минут. Цифры "
        f"обновляются по мере прихода новых событий и закрытия 5-минутных окон."
    )

st.divider()

st.subheader("🏆 Топ за всё время")
st.caption(
    "Это весь топ, посчитанный по всем 5-минутным окнам с момента запуска "
    "pipeline. Не путать со streaming-окнами: здесь окна нет, агрегат — за всё."
)

all_time_df = query_df(
    """
    SELECT movie_id,
           MAX(title)  AS title,
           MAX(genres) AS genres,
           SUM(rating_count) AS rating_count,
           ROUND((SUM(avg_rating * rating_count) / NULLIF(SUM(rating_count), 0))::numeric, 2) AS avg_rating
    FROM trending_movies
    GROUP BY movie_id
    ORDER BY rating_count DESC
    LIMIT 20
    """
)
if all_time_df.empty:
    st.info("Данных нет.")
else:
    all_time_df.insert(0, "Топ", [f"#{i + 1}" for i in range(len(all_time_df))])
    all_time_df = all_time_df.rename(columns={
        "movie_id": "ID",
        "title": "Название",
        "genres": "Жанры",
        "rating_count": "Оценок всего",
        "avg_rating": "Средний рейтинг",
    })
    st.dataframe(all_time_df, hide_index=True, use_container_width=True)

st.caption(
    "**Почему «топ за всё время» странный:** это всё ещё только то, что прошло "
    "через pipeline с момента запуска (а не за все 25 лет MovieLens). "
    "Чтобы посчитать вечный all-time топ — нужен batch-job по сырому "
    "`ratings.csv`, без Kafka. Streaming нужен именно для «что популярно сейчас»."
)

st.divider()

st.subheader("🎭 Топ-жанры за последние 24 часа")
st.caption(
    "То же самое, но за окна 1 час и в разрезе жанров. "
    "Бар-чарт слева — топ-жанры по количеству оценок."
)

genres_df = query_df(
    """
    SELECT genre AS "Жанр",
           SUM(rating_count) AS "Оценок",
           ROUND((SUM(avg_rating * rating_count) / NULLIF(SUM(rating_count), 0))::numeric, 2) AS "Средний рейтинг"
    FROM top_genres
    WHERE window_start >= NOW() - INTERVAL '24 hours'
    GROUP BY genre
    ORDER BY "Оценок" DESC
    LIMIT 20
    """
)
if genres_df.empty:
    st.info("За последние сутки жанровой активности ещё не было.")
else:
    chart_col, table_col = st.columns([3, 2])
    with chart_col:
        st.bar_chart(genres_df.set_index("Жанр")["Оценок"], height=400)
    with table_col:
        st.dataframe(genres_df, hide_index=True, use_container_width=True)

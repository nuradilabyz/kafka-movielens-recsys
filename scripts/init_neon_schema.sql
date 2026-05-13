-- Schema for the Neon Postgres sink.
-- Apply with: psql "$NEON_DSN" -f scripts/init_neon_schema.sql

-- Per-user latest top-N recommendations. UPSERT by user_id keeps the table bounded.
CREATE TABLE IF NOT EXISTS user_recs (
    user_id      BIGINT PRIMARY KEY,
    recs         JSONB NOT NULL,
    user_vec     REAL[],
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE user_recs ADD COLUMN IF NOT EXISTS user_vec REAL[];
CREATE INDEX IF NOT EXISTS user_recs_updated_at_idx ON user_recs (updated_at DESC);

-- Tumbling-window trending movies emitted by ksqlDB sink consumer.
CREATE TABLE IF NOT EXISTS trending_movies (
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    movie_id     BIGINT NOT NULL,
    title        TEXT,
    genres       TEXT,
    rating_count BIGINT NOT NULL,
    avg_rating   DOUBLE PRECISION,
    PRIMARY KEY (window_start, movie_id)
);
CREATE INDEX IF NOT EXISTS trending_movies_window_idx ON trending_movies (window_start DESC, rating_count DESC);

-- Genre leaderboard per hour window.
CREATE TABLE IF NOT EXISTS top_genres (
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    genre        TEXT NOT NULL,
    rating_count BIGINT NOT NULL,
    avg_rating   DOUBLE PRECISION,
    PRIMARY KEY (window_start, genre)
);
CREATE INDEX IF NOT EXISTS top_genres_window_idx ON top_genres (window_start DESC, rating_count DESC);

-- Enriched event stream tail (last N events) for the UI to show "live activity".
CREATE TABLE IF NOT EXISTS events_enriched (
    id           BIGSERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL,
    movie_id     BIGINT NOT NULL,
    title        TEXT,
    genres       TEXT,
    rating       REAL NOT NULL,
    event_ts     TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS events_enriched_ingested_idx ON events_enriched (ingested_at DESC);

-- Pipeline metrics heartbeat written by the recommender consumer.
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    component       TEXT PRIMARY KEY,
    last_offset     BIGINT,
    messages_total  BIGINT NOT NULL DEFAULT 0,
    last_event_ts   TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

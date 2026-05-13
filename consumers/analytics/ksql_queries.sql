-- ksqlDB streaming SQL for the analytics topics.
-- Apply via: docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 < ksql_queries.sql

SET 'auto.offset.reset' = 'earliest';

-- 1. Source stream over the raw ratings topic.
-- We intentionally use the Kafka record timestamp (wall-clock at produce time)
-- instead of the historical `ts` field, so window boundaries match demo time
-- ("trending in the last 5 wall-clock minutes") rather than 2019.
CREATE STREAM IF NOT EXISTS ratings_raw (
    user_id  BIGINT,
    movie_id BIGINT,
    rating   DOUBLE,
    ts       BIGINT
) WITH (
    KAFKA_TOPIC = 'movielens.ratings.raw',
    VALUE_FORMAT = 'JSON'
);

-- 2. Reference table of movies (loaded once into a compacted topic by a sink job).
--    For the demo we instead enrich on the consumer side; this table definition
--    is here so a future iteration can join in ksqlDB directly.
CREATE TABLE IF NOT EXISTS movies_ref (
    movie_id BIGINT PRIMARY KEY,
    title    VARCHAR,
    genres   VARCHAR
) WITH (
    KAFKA_TOPIC = 'movielens.movies.ref',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 6
);

-- 3. Tumbling 5-minute window per movie: rating count + average rating.
CREATE TABLE IF NOT EXISTS trending_movies_5m
WITH (KAFKA_TOPIC = 'movielens.trending.5m', VALUE_FORMAT = 'JSON', PARTITIONS = 6) AS
SELECT
    movie_id,
    COUNT(*)    AS rating_count,
    AVG(rating) AS avg_rating,
    WINDOWSTART AS window_start,
    WINDOWEND   AS window_end
FROM ratings_raw
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY movie_id
EMIT CHANGES;

-- 4. Hourly user activity (used by the UI's pipeline page).
CREATE TABLE IF NOT EXISTS user_activity_1h
WITH (KAFKA_TOPIC = 'movielens.user_activity.1h', VALUE_FORMAT = 'JSON', PARTITIONS = 6) AS
SELECT
    user_id,
    COUNT(*)    AS rating_count,
    AVG(rating) AS avg_rating,
    WINDOWSTART AS window_start,
    WINDOWEND   AS window_end
FROM ratings_raw
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY user_id
EMIT CHANGES;

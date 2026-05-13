# Kafka Portfolio Project — Data Availability Audit for 7 Project Ideas

**TL;DR:** Project **#2 Market Firehose** has by far the best public data (Binance's official public archive = real, tick-level, multi-symbol, free, terabytes available). **#3 Fraud Shield** and **#4 Media Recs** are tied for second on data quality. **#6 Ticketon Queue** and **#7 Customer 360** have the weakest realistic data — both essentially require synthesizing/blending datasets, which hurts the "production-like" signal you want for Freedom/Kaspi/Halyk interviews.

If your goal is "credibly demonstrates Kafka at scale," go with **Market Firehose on Binance public data**, with **Fraud Shield on IEEE-CIS** as the runner-up.

---

## Project 1 — Cashback Valuator (stream-stream join: transactions ⨝ stock ticks)

**What you need:** two streams with overlapping timestamps — high-frequency ticks for at least one symbol + a transaction stream with user IDs/amounts.

**Candidates**

1. **Huge Stock Price Data: Intraday Minute Bar** (Kaggle, `arashnic/stock-data-intraday-minute-bar`)
   - Minute-level OHLCV for many US stocks.
   - Timestamp granularity: 1-minute bars.
   - Size: ~400–600MB depending on snapshot.
   - Prior usage: appears in dozens of Kaggle notebooks and student projects; not heavily cited in academic papers.
   - Limitation: minute bars, not true ticks. Fine for portfolio.

2. **Boris Marjanovic "Huge Stock Market Dataset"** (`borismarjanovic/price-volume-data-for-all-us-stocks-etfs`)
   - ~7000 US stocks/ETFs, **daily OHLCV only**. Schema: Date, Open, High, Low, Close, Volume.
   - Size: ~700MB unpacked.
   - Prior usage: very popular Kaggle dataset, hundreds of notebooks.
   - **Don't use as the tick source — too coarse for "live ticks."**

3. **Binance public data** (`data.binance.vision`) — aggTrades and 1m klines per symbol, free, no auth. Use a crypto pair (e.g., BTCUSDT) as a stand-in "FRHC" ticker; real sub-second ticks, ~hundreds of MB per symbol per month.

4. **Transactions side:** pair with **IEEE-CIS Fraud Detection** (`TransactionDT` is seconds-since-reference; 590K rows over 6 months, real Vesta e-commerce data) or **Sparkov** (`kartik2112/fraud-detection`, synthetic but real-looking timestamps + amounts + user IDs).

**Realism / fit:** ★★★. Two streams have **no real causal relation** (you're synthetically joining unrelated data), so the business story is weak — but the Kafka pipeline is honest and reviewers will accept it.

**Limitation:** the join is contrived. Pitch it as "enrich each transaction with the prevailing FRHC price at txn-time to compute cashback in shares."

---

## Project 2 — Market Firehose (multi-symbol, high-throughput) — **DATA WINNER**

**Candidates**

1. **Binance public data archive** — `https://data.binance.vision/` (docs at `github.com/binance/binance-public-data`)
   - **Real exchange data**, freely downloadable as zipped CSVs.
   - Three relevant streams: `trades` (every executed trade), `aggTrades` (aggregated executions per price level), `klines` (candles from 1m up).
   - Schema for aggTrades: `agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time (ms unix), is_buyer_maker`.
   - Timestamp granularity: **millisecond** (microsecond from Jan 2025 for spot).
   - Volume: ETHUSDT alone produced ~892M tick records from Mar 2021 – May 2023. Just download 1–3 months of a top-5 pair to easily get several GB.
   - Prior usage: heavily used by quant/HFT researchers and trading bots; the canonical public crypto-market dataset. CryptoDataDownload, binance_historical_data PyPI package, and binance-historical npm package all built around it.
   - Licence: free for non-commercial use per Binance terms.

2. **LOBSTER** (`lobsterdata.com`) — reconstructed NASDAQ Level-3 limit order book from TotalView-ITCH.
   - Schema: separate "message" and "orderbook" files, columns include time-of-day (seconds since midnight, ms resolution), event type, order ID, size, price, direction; orderbook files give N levels of bid/ask prices and volumes.
   - Timestamp granularity: **millisecond** (NASDAQ native).
   - Free samples available for AMZN, AAPL, GOOG (June 21, 2012, full trading day, all message types). Sample size: ~100–400MB per stock per day depending on depth levels.
   - Prior usage: cited in 100+ academic papers (Bonart & Gould 2016 arXiv:1511.04116; Huang & Polak SSRN; many ML-for-LOB papers). Highly credible.
   - Limitation: full universe access requires academic affiliation request.

3. **Huge Stock Market Dataset (Boris Marjanovic)** — daily only, weak for "firehose." Skip.

**Realism / fit:** ★★★★★. This is literally what an exchange feed looks like.

**Kafka angle:** one topic per symbol (or one topic partitioned by symbol), watchlist-filtering as a Kafka Streams/ksqlDB consumer; partition by symbol for parallelism. Clear opportunities to demonstrate consumer groups, partitioning strategy, back-pressure.

---

## Project 3 — Cross-Service Fraud Shield

**Candidates**

1. **IEEE-CIS Fraud Detection** (Kaggle competition, dataset by Vesta Corporation) — **gold standard for portfolio fraud projects.**
   - 590,540 real card-not-present transactions over ~6 months, 3.5% fraud rate (`isFraud` label), 433+ feature columns.
   - Two joined files: `transaction.csv` (TransactionID, **TransactionDT** = seconds-since-reference, TransactionAmt, ProductCD, card1–card6, addr1/addr2, dist1/dist2, P_emaildomain, R_emaildomain, C1–C14, D1–D15, M1–M9, V1–V339 engineered features) and `identity.csv` (DeviceType, DeviceInfo, id_01…id_38 — includes anonymized IP-derived signals).
   - Size: train ~650MB unpacked, test ~470MB. Total ~1.2GB. **Right in your size sweet spot.**
   - Prior usage: top Kaggle competition (~6,500 teams), still actively benchmarked in 2024–2025 papers (Preprints.org case study, arXiv 2512.21866 dataset distillation paper, FDB paper arXiv:2208.14417).

2. **Sparkov / Credit Card Transactions Fraud Detection** (`kartik2112/fraud-detection` on Kaggle, CC0 licence)
   - Synthetic, generated by the Sparkov data generator (`github.com/namebrandon/Sparkov_Data_Generation`). 1000 customers, 800 merchants, 6 months.
   - Schema: `trans_date_trans_time, cc_num, merchant, category, amt, first, last, gender, street, city, state, zip, lat, long, city_pop, job, dob, trans_num, unix_time, merch_lat, merch_long, is_fraud`.
   - **Has full timestamps + user IDs + geo.** No IPs.
   - Size: ~340MB (train + test).
   - Prior usage: ensemble-learning paper Akinrolabu et al. 2024 ResearchGate 379191633; included as `sparknov` in Amazon's Fraud Dataset Benchmark.

3. **Amazon FDB (Fraud Dataset Benchmark)** — `github.com/amazon-science/fraud-dataset-benchmark` curates 9 fraud datasets. Per their paper: only `fraudecom` + `ipblock` have raw IPs; only `ieeecis, ccfraud, fraudecom, sparknov` have timestamps; only `ieeecis, fraudecom, sparknov` have user IDs. **No single public dataset has clean timestamps + user IDs + IPs + real fraud labels.**

**Realism / fit:** ★★★★ for IEEE-CIS, ★★★ for Sparkov.

**Limitation:** Plan to synthesize IP/user-agent for IEEE-CIS rows to tell the "login from new IP → high-value purchase" story.

---

## Project 4 — AI-Driven Freedom Media Recommendations

**Candidates**

1. **MovieLens 25M** (`grouplens.org/datasets/movielens/`, also Kaggle mirror)
   - 25,000,095 ratings + 1,093,360 tag applications across 62,423 movies by 162,541 users between Jan 9, 1995 and Nov 21, 2019.
   - Schema: `ratings.csv` (`userId, movieId, rating, timestamp`), `movies.csv` (`movieId, title, genres`), `tags.csv` (`userId, movieId, tag, timestamp`), `links.csv` (IMDb/TMDb mappings), `genome-scores.csv`, `genome-tags.csv`.
   - Timestamp granularity: **second** (unix epoch). Perfect for time-replay.
   - Size: ~250MB zipped, ~1GB unpacked.
   - Prior usage: literally **thousands** of recommender-system papers; the canonical recsys benchmark.

2. **MovieLens 32M** — newer, released May 2024. 32M ratings + 2M tag applications across 87,585 movies by 200,948 users, collected through Oct 2023. Same schema. Use this if you want bragging rights for "most recent."

3. **Other viable options** (didn't deep-dive): Yelp Open Dataset, Amazon Reviews, Last.fm 1K, YooChoose RecSys 2015, Twitch SNAP datasets. MovieLens still wins on cleanliness of replay story.

**Realism / fit:** ★★★★. Ratings ≠ "watch events" but the schema is identical to what Kinopoisk/IVI/Freedom Media would actually have. Frame it as "view-completion events."

**Kafka angle:** producer replays `ratings.csv` ordered by `timestamp`, partition by `userId`, consumer maintains per-user feature store (Redis) and emits "top-N" updates; you can plug your existing **Qdrant** for vector-based candidate retrieval — bonus signal for a Junior AI Engineer role.

---

## Project 5 — Arbuz Flash Sale Engine

**Candidates**

1. **Instacart Online Grocery Basket Analysis** (Kaggle competition + `psparks/instacart-market-basket-analysis`)
   - ~3M grocery orders, 200K+ users, 50K products, 6 CSV tables (orders, order_products__prior/train, products, aisles, departments).
   - Schema includes `order_dow, order_hour_of_day, days_since_prior_order` — **but no absolute timestamp**, only relative weekday/hour. You'd have to anchor a synthetic epoch to replay it.
   - Size: ~200MB unpacked.
   - Prior usage: 2017 Kaggle competition, ~3,000 teams; widely used in market-basket / association-rules teaching (Databricks notebook gallery, SESUG paper 252-2019).

2. **Olist Brazilian E-Commerce** (`olistbr/brazilian-ecommerce`) — **best schema for stream-events.**
   - 100K real orders, 2016–2018, multi-marketplace. Multiple files: orders, order_items, payments, reviews, customers, sellers, products, geolocation.
   - **Has true timestamps**: `order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, shipping_limit_date, review_creation_date, review_answer_timestamp`. Multi-event-per-order is gold for Kafka topics.
   - Real commercial data (anonymized; company references replaced with Game of Thrones house names).
   - Size: ~120MB.
   - Prior usage: extremely popular for end-to-end data engineering tutorials (numerous Medium articles, Tableau dashboards, AWS S3/Redshift pipelines).

3. **Dunnhumby "The Complete Journey"** (`kaggle.com/datasets/frtgnn/dunnhumby-the-complete-journey`, also from 84.51° directly)
   - 2,469 households, **2 years** of frequent-shopper transactions: ~1.47M transaction rows + 20.94M promotion rows. Real retailer data.
   - Schema: `household_id, store_id, basket_id, product_id, quantity, sales_value, retail_disc, coupon_disc, coupon_match_disc, week, transaction_timestamp` plus campaign / coupon / hh_demographic / product tables.
   - **Has `transaction_timestamp` (datetime).** Plus promotions and 30 campaigns — excellent for the dynamic-pricing/coupons story.
   - Size: ~400MB unpacked (transactions table is the big one).
   - Prior usage: well-known in retail analytics teaching; `completejourney` R package on CRAN; multiple churn-prediction / market-basket case studies.

**Realism / fit:** ★★★★ for Olist + Dunnhumby. For Arbuz (grocery), **Dunnhumby is the best thematic match**.

**Limitation:** None of them ship explicit inventory snapshots — you reconstruct inventory by replaying transactions and assuming starting stock. That's a feature, not a bug, for your project: textbook event-sourcing / materialized-view exercise.

---

## Project 6 — Unified Ticketon/Aviata Queue ⚠️ WEAKEST DATA AVAILABILITY

**Public booking data with seat-level granularity and high-concurrency patterns essentially does not exist as an open dataset.** Be honest about this in your portfolio README.

**Candidates (all imperfect)**

1. **Expedia Hotel Recommendations** (Kaggle competition, ~3.9 GB)
   - ~300M training events, ~75M test events of real user click/search/book sessions on Expedia, plus 62K destinations table.
   - Schema: `date_time, site_name, posa_continent, user_location_*, user_id, is_mobile, is_package, channel, srch_ci, srch_co, srch_adults_cnt, srch_children_cnt, srch_rm_cnt, srch_destination_id, hotel_continent, hotel_country, hotel_market, is_booking, hotel_cluster`, etc.
   - Timestamp granularity: per-event datetime.
   - Prior usage: Kaggle 2016 competition (~1,970 teams); arXiv 1703.02915, 1908.07498 papers.
   - **Doesn't have seat/room-level "hot inventory"** but it's the closest real-booking dataset at scale.

2. **Hotel booking demand** (`saadharoon27/hotel-booking-dataset` and similar; Antonio, Almeida & Nunes "Data in Brief" paper) — ~119,390 rows, has `arrival_date_*`, lead time, ADR, 32 columns. Small but real. Too small for "high concurrency."

3. **Hotel Reservations Classification** (`ahsan81/hotel-reservations-classification-dataset`) — similar size and limitations.

4. **No usable public airline-booking or concert-ticket dataset with seat IDs exists** that I could find. US BTS provides aggregate flight performance, not bookings. Most realistic option: **generate your own** booking pattern from a flight schedule (BTS data) using Sparkov-style synthetic generation, and own the synthetic-ness in the README.

**Realism / fit:** ★★ at best. Kafka requirements (strong ordering, hot-key contention, instant cross-system propagation) are interesting engineering problems but you'd be demonstrating them on weakly-fitting data.

**Recommendation:** Skip this project unless you're prepared to write your own booking simulator.

---

## Project 7 — Unified Customer 360 Event Bus ⚠️ ALSO WEAK

**Problem:** no single public dataset spans Bank ⨯ Grocery ⨯ Airline with shared user IDs. Two options:

1. **Stitch multiple datasets with synthetic linking IDs** — e.g., generate a `master_user_id`, randomly map subsets onto IEEE-CIS users, Olist customers, MovieLens users, hotel-booking users. Each dataset becomes one "service" in the event bus. This is what most CDC/Customer-360 portfolio projects actually do.

2. **Santander Customer Transaction Prediction** (Kaggle) — 200K rows × 200 features, **but features are fully anonymized to `var_0...var_199`** with no semantic meaning and **no timestamps**. Useless for streaming replay. Skip.
   - Size: ~300MB.
   - Prior usage: Kaggle 2019 competition (~8,800 teams).

3. **Real CRM/banking customer datasets** (Lloyds/Santander competitions, telco churn) — almost all are static snapshots with no event timestamps. Not designed for replay.

**Realism / fit:** ★★. Engineering story is great (CDC, Debezium-style change events, schema registry, GDPR-style deletes) but the data foundation is essentially constructed by you.

**Recommendation:** If you want this story, do it as a **secondary layer on top of one of the strong projects** — e.g., build Market Firehose and add a "Customer 360" view that joins user-trade-events with synthesized profile data.

---

## Coverage Confirmation

| Project | 1-3 candidates | Source/URL | Size | Schema | Timestamp granularity | Realism rating | Prior usage | Limitations |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 Cashback | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 2 Firehose | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 3 Fraud | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 4 Recs | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 5 Flash Sale | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 6 Queue | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 7 360 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

---

## Ranked Final Recommendation

| Rank | Project | Data Quality | Realism | Replay-ability | Difficulty | Portfolio Signal |
|------|---------|:---:|:---:|:---:|:---:|:---:|
| 🥇 1 | **#2 Market Firehose** | ★★★★★ | ★★★★★ | ★★★★★ | High | Very high |
| 🥈 2 | **#3 Fraud Shield** (IEEE-CIS) | ★★★★ | ★★★★ | ★★★★ | Medium | Very high — directly relevant to **Freedom Insurance, Kaspi, Halyk** |
| 🥉 3 | **#4 Media Recs** (MovieLens 32M) | ★★★★ | ★★★ | ★★★★★ | Medium | High — pairs naturally with your Qdrant/RAG background |
| 4 | **#5 Flash Sale** (Dunnhumby/Olist) | ★★★★ | ★★★★ | ★★★ | Medium-High | High |
| 5 | **#1 Cashback Valuator** | ★★★ | ★★ (contrived join) | ★★★★ | Medium | Medium |
| 6 | **#7 Customer 360** | ★★ | ★★ | ★★ | High | Medium |
| 7 | **#6 Ticketon Queue** | ★ | ★ | ★★ | High | Low — too synthetic |

### My pick for you: **#2 Market Firehose on Binance public data**

**Why this beats the rest for your job hunt:**
- Real exchange data → no "is this synthetic?" objections in an interview
- Trivially scales to many GB → demonstrates partitioning, consumer groups, back-pressure, exactly-once
- KASE/Freedom-relevant business framing (market data is core to Freedom Finance/Freedom Insurance)
- Kafka throughput numbers will be impressive (millions of events/sec is achievable on a laptop)
- Pairs naturally with **Project #3 Fraud Shield** as a follow-up for a 2-project portfolio — same exchange-paradigm, different domain

**Concrete starter pack**

| Item | Value |
|---|---|
| Dataset | Binance Spot aggTrades, top 5 pairs (BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT) |
| URL | `https://data.binance.vision/?prefix=data/spot/monthly/aggTrades/` |
| Download | One month per pair as zipped CSV. e.g. `curl -s https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-12.zip -o btcusdt.zip` |
| Approx size | 1–3 GB total for 5 pairs × 1 month |
| Auth | None |
| Licence | Free for non-commercial use per Binance terms |
| Schema | `agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time (ms), is_buyer_maker` |
| Helper lib | `pip install binance-historical-data` for bulk dumping |

**Suggested Kafka topology**

```
                ┌────────────────────────────────┐
producer.py ──► │ topic: market.ticks.raw        │ (5 partitions, key = symbol)
(replays CSV    └────────────────┬───────────────┘
in transact_time                 │
order, configurable              ▼
speed-up factor)        ┌─────────────────────┐
                        │ Kafka Streams /     │
                        │ ksqlDB / Flink      │  ── windowed aggregation: 1m/5m OHLCV
                        └────────┬────────────┘
                                 │
            ┌────────────────────┼──────────────────────┐
            ▼                    ▼                      ▼
  topic: market.ohlcv.1m   topic: alerts.user.X    topic: market.vwap
  (per-symbol bars)        (watchlist filter)      (rolling VWAP)
            │                    │                      │
            ▼                    ▼                      ▼
        Postgres /           websocket /             Postgres /
        Parquet sink         frontend push           Qdrant (vector
                                                     similarity on
                                                     price regimes)
```

**Components to demonstrate (this is what gets you hired):**
1. **Producer:** Python or Go script that reads the CSV, sorts by `transact_time`, sleeps `(t_event - t_event_prev) / speed_factor` between sends. Speed-up = "replay 1 day in 1 hour."
2. **Schema Registry + Avro/Protobuf** for the tick schema.
3. **Partitioning by symbol** so a single consumer per partition gets ordered ticks per symbol.
4. **Kafka Streams or ksqlDB** for tumbling-window OHLCV and rolling VWAP.
5. **Watchlist filter** as a stateful consumer (the "filtered firehose per user" requirement) — store watchlists in a compacted Kafka topic for hot reload.
6. **Sink connectors** to PostgreSQL (where you already have skills) and optionally Qdrant for vector-similarity on price regimes (uses your RAG background).
7. **Docker Compose** (you already know Docker) with Kafka + Schema Registry + Postgres + a small Streamlit/Next.js UI.
8. **Bonus:** load-test with Locust hitting the producer to demonstrate back-pressure and consumer-lag dashboards in Grafana.

**Why this is interview-perfect for Freedom Insurance Data Engineer / Kaspi / Halyk:**
- Freedom Finance literally trades on KASE/Nasdaq — market data is their bread and butter
- Kaspi and Halyk both have real-time payment streams that look architecturally identical to a trade feed
- The "watchlist filtering" requirement maps cleanly onto "fraud rule routing" or "transaction-alert routing" in banking — same Kafka primitives

### Strong alternative if you want banking-flavor over markets-flavor:

Run **#3 Fraud Shield on IEEE-CIS** with the same architecture. Replace symbol with `card_id`, replace OHLCV with "rolling spend windows," replace VWAP with "running fraud-score." Same pipeline, more directly relevant to Kaspi/Halyk fraud teams. Real Vesta e-commerce transactions, well-known competition, dataset is ~1.2GB so you stay in your size sweet spot.

---

**Pitfalls to avoid in all cases:**
- Don't use Boris Marjanovic's daily dataset for "ticks" — too coarse.
- Don't use Santander Customer Transaction Prediction — anonymized features, no timestamps, no story.
- Don't claim Sparkov data is real — own the synthetic nature and explain *why* it's still useful (only one with full PII-style fields).
- For MovieLens, frame ratings as "view-completion events" — don't pretend they're literal "watch events."
- For Instacart, anchor a synthetic epoch — the dataset only has weekday/hour, not absolute timestamps.
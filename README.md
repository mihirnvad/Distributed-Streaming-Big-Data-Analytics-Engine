# Real-Time Payment Fraud Detection Lakehouse

[![CI](../../actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)

An end-to-end streaming analytics platform that ingests card-payment events from **Kafka**, processes
them with **Spark Structured Streaming** through a **Bronze → Silver → Gold medallion lakehouse on
Delta Lake**, scores every transaction with a stateful, explainable fraud engine, and serves a
**PostgreSQL star schema** to a library of analytical SQL models and a live **Streamlit** operations
dashboard.

Everything runs locally with one command, and every layer is covered by tests that run in CI.

```
docker compose up -d --build        # then open http://localhost:8501
```

---

## What it does

| | |
|---|---|
| **Ingest** | A multi-process producer generates realistic card traffic (100k cardholders, 2.5k merchants, 36 cities, 17 currencies) and injects four fraud patterns, legitimate-but-suspicious behaviour, delayed chargeback labels, duplicates, late events and malformed payloads. |
| **Bronze** | Raw Kafka records land in Delta exactly once, keyed by their Kafka coordinates, so the lakehouse (not Kafka retention) is the replay source. |
| **Silver** | Explicit-schema parsing, 9 data-quality rules with a quarantine table, currency normalisation, and watermarked cross-batch deduplication. |
| **Gold** | A per-card **stateful feature engine** (`applyInPandasWithState`) computes trailing-window velocity, impossible-travel speed and amount z-scores; a config-driven **rule engine** scores every transaction with reason codes; **5-minute sliding windows** aggregate merchant risk. |
| **Serve** | Idempotent COPY + `INSERT … ON CONFLICT` upserts into a day-partitioned Postgres star schema (BRIN + B-tree indexes, SCD Type 2 cardholders). |
| **Analyse** | 15 analytical SQL models (CTEs, window functions, gaps-and-islands, ROLLUP, empirical-Bayes smoothing, threshold tuning curves) and a live dashboard. |
| **Operate** | Every micro-batch's latency, throughput, watermark, state size and Kafka lag is persisted for SLO tracking; a nightly batch job reconciles the warehouse against the lakehouse and compacts Delta tables. |

---

## Architecture

```mermaid
flowchart LR
    subgraph SRC["Event source"]
        P["Transaction producer<br/>multi-process, keyed by user_id<br/>fraud + chaos injection"]
    end

    subgraph K["Kafka (KRaft)"]
        T1[("payments.transactions<br/>6 partitions")]
        T2[("payments.chargebacks<br/>2 partitions")]
    end

    subgraph SPARK["Spark Structured Streaming - one app, 8 queries, FAIR pools per layer"]
        direction LR
        B["BRONZE<br/>raw payload + Kafka offsets<br/>exactly-once Delta sink"]
        S["SILVER<br/>explicit schema, 9 DQ rules<br/>USD normalisation<br/>dropDuplicatesWithinWatermark"]
        Q["QUARANTINE<br/>rejected rows + reasons"]
        G["GOLD<br/>applyInPandasWithState per card<br/>dimension enrichment<br/>rule engine + reason codes"]
        W["GOLD AGGREGATES<br/>5-min / 1-min sliding windows<br/>per merchant, UPDATE mode"]
    end

    subgraph LAKE["Delta Lake (system of record)"]
        D1[("bronze.*")]
        D2[("silver.*")]
        D3[("gold.scored_transactions")]
    end

    subgraph PG["PostgreSQL serving layer"]
        F[("dw star schema<br/>fact_transactions (daily partitions)<br/>fact_fraud_alerts, fact_chargebacks<br/>dim_users SCD2, dim_merchants, ...")]
        O[("ops.streaming_query_progress<br/>ops.reconciliation_runs")]
    end

    P --> T1 & T2
    T1 & T2 --> B --> D1
    D1 --> S --> D2
    S --> Q
    D2 --> G --> D3
    D3 --> W
    G -- "COPY + ON CONFLICT" --> F
    W -- "upsert" --> F
    SPARK -. "StreamingQueryListener" .-> O
    F --> DASH["Streamlit dashboard"]
    F --> SQL["Analytical SQL models"]
    O --> DASH
    D3 -. "nightly reconciliation<br/>backfill, hourly aggregates,<br/>OPTIMIZE / VACUUM" .-> F
```

### Services (`docker-compose.yml`)

| Service | Image | Purpose | UI |
|---|---|---|---|
| `kafka` + `kafka-init` | `apache/kafka:4.1.2` | KRaft broker (no ZooKeeper); topics created idempotently | – |
| `postgres` | `postgres:17-alpine` | Serving warehouse; schema applied on first start | `localhost:5432` |
| `seed` | app image | Loads reference data + synthetic population into the dimensions (SCD2 merge) | – |
| `spark-master`, `spark-worker-1/2` | Spark 4.1.3 image | Standalone cluster, 2 × 2 cores | [`:8080`](http://localhost:8080) |
| `pipeline` | Spark 4.1.3 image | Streaming driver running all medallion queries | [`:4040`](http://localhost:4040) |
| `producer` | app image | Synthetic payment + chargeback events | – |
| `dashboard` | app image | Streamlit operations dashboard | [`:8501`](http://localhost:8501) |
| `reconcile` *(profile `batch`)* | Spark image | Nightly reconciliation / maintenance job | – |
| `kafka-ui`, `adminer` *(profile `tools`)* | – | Optional admin UIs | `:8090`, `:8091` |

---

## Engineering decisions (the interesting parts)

**Exactly-once, end to end.**
Kafka offsets for each micro-batch are written to the checkpoint's write-ahead log before processing,
and Delta sinks record the batch id atomically, so bronze/silver are exactly-once. The gold
`foreachBatch` sink is made exactly-once *effectively*: Delta writes carry
`txnAppId`/`txnVersion` (the app id is derived from the checkpoint's query id, so a deliberately reset
checkpoint cannot be mistaken for a replay), and Postgres writes are keyed upserts, so replaying a
batch after a crash is a no-op. Spark tracks Kafka offsets in the checkpoint rather than in consumer
groups; lag is exposed through source metrics into `ops.streaming_query_progress`.

**Bounded-state deduplication.** Producers retry, so duplicates are byte-identical replays.
`dropDuplicatesWithinWatermark` removes them *across* micro-batches while the 10-minute event-time
watermark bounds the state (plain `dropDuplicates` on a stream keeps keys forever). The warehouse
primary key is a second line of defence.

**Per-event features need arbitrary state, not windows.** A tumbling/sliding window answers "how many
swipes between 10:00 and 10:05"; scoring needs "how many in the five minutes *before this swipe*" and
"how fast would the cardholder have travelled since the previous swipe". `streaming/features.py`
keeps a small per-card state (recent events, last position, Welford mean/variance of log-amount,
EWMA of inter-arrival gaps) inside `applyInPandasWithState`, with event-time timeouts evicting idle
cards. The engine core is plain Python, so it is unit-tested in milliseconds.

**Velocity relative to the card's own baseline.** `VELOCITY_BURST` fires when the last-minute count
exceeds 4× the card's learned rate, falling back to stricter absolute thresholds while the baseline
warms up. Heavy but legitimate users are not flagged for being heavy.

**Sliding windows that are both real-time and correct.** Merchant windows (5 minutes, sliding every
minute) run in `update` output mode and are upserted with `ON CONFLICT DO UPDATE`: dashboards see a
window's running value within seconds, and late events refine it until the watermark finalises it.
Spike detection compares each window with the merchant's own trailing baseline in SQL.

**Explainable, config-driven scoring.** Rules are native Spark column expressions (no Python UDF)
registered by name; weights and thresholds live in [`config/fraud_rules.yaml`](config/fraud_rules.yaml).
Signals combine with a noisy-OR (`1 − Π(1 − wᵢ)`), and every row carries `reason_codes` so an analyst
can see *why* it was flagged, and so rule precision/recall can be measured per rule.

**Parse each payload once.** `select(from_json(...).alias("j")).select("j.*")` looks harmless, but
Catalyst inlines the `from_json` call into every field reference and pushed-down filter: the physical
plan contained **23 `from_json` calls per record**. Parsing through a generator
(`inline(array(from_json(...)))`) is an optimisation barrier and brings it to **1**
(see `streaming/silver_transforms.py::_parse_payload`).

**The lakehouse is the system of record.** Postgres is a serving copy (it even runs with
`synchronous_commit = off` for ingest throughput). The nightly job
[`batch/historical_reconciliation.py`](batch/historical_reconciliation.py) anti-joins the gold Delta table
against the warehouse, backfills gaps, rebuilds hourly aggregates, resolves alert status from
chargebacks, OPTIMIZEs/VACUUMs the day's Delta partitions and writes an audit row.

**Warehouse design.** `fact_transactions` is range-partitioned by day (retention is `DROP PARTITION`);
a function pre-creates partitions and safely moves stray rows out of the DEFAULT partition.
BRIN indexes on the append-ordered timestamps are a few KB instead of hundreds of MB; B-tree
indexes serve per-card and per-merchant lookups; a partial index serves the alert feed. Unknown
dimension members map to `-1` rows so a brand-new card is still scored and loaded.

**Laptop-friendly by design.** Dimensions are cached on executors and refreshed every 5 minutes
rather than re-read per batch; Delta's snapshot reconstruction is sized for a 4-core cluster;
connector JARs are resolved at image build time (skipping anything Spark already ships, and pinning
Jackson modules to Spark's version) so containers start offline.

---

## Fraud scenarios and rules

The simulator injects **labelled** fraud; labels reach the pipeline only through **delayed
chargebacks** (~85% of fraud gets reported, and some genuine purchases are disputed as "friendly
fraud"), exactly like production.

| Scenario | Pattern | Rules expected to catch it |
|---|---|---|
| Card testing | 5–12 sub-$5 card-not-present charges at different merchants within ~1 min, then a $400–2,500 cash-out | `CARD_TESTING`, `IMPOSSIBLE_TRAVEL`, `HIGH_RISK_MCC_LARGE_TICKET` |
| Account takeover | 1–3 large online purchases from a foreign IP location | `AMOUNT_SPIKE`, `HIGH_RISK_MCC_LARGE_TICKET`, `IMPOSSIBLE_TRAVEL` |
| Cloned card | Genuine chip swipe at home, then magstripe swipes 3,000+ km away minutes later | `IMPOSSIBLE_TRAVEL`, `MAGSTRIPE_ON_CHIP_CARD`, `FAR_FROM_HOME_CARD_PRESENT` |
| Spending spree | Stolen card tapped at 8–14 local shops in a few minutes | `VELOCITY_BURST` |
| *Legitimate* big tickets, travel, friendly fraud | Sources of false positives / label noise | – |

| Rule | Weight | Fires when |
|---|---|---|
| `VELOCITY_BURST` | 0.60 | last-minute / 5-minute count ≫ the card's learned rate |
| `CARD_TESTING` | 0.70 | ≥4 sub-$5 charges at ≥3 merchants in 5 minutes |
| `AMOUNT_SPIKE` | 0.45 | z-score of log(amount) vs. the card's history ≥ 3 |
| `IMPOSSIBLE_TRAVEL` | 0.75 | implied speed since previous swipe > 900 km/h over ≥ 300 km |
| `HIGH_RISK_MCC_LARGE_TICKET` | 0.30 | ≥ $750 in electronics, jewelry, gambling or crypto |
| `FAR_FROM_HOME_CARD_PRESENT` | 0.20 | card-present ≥ 1,500 km from home |
| `MAGSTRIPE_ON_CHIP_CARD` | 0.25 | swipe on a chip card (counterfeit signature) |
| `NEW_ACCOUNT_HIGH_VALUE` | 0.25 | ≥ $500 on an account younger than 30 days |

Risk bands: `CRITICAL ≥ 0.85`, `HIGH ≥ 0.60` (alert threshold), `MEDIUM ≥ 0.30`.

---

## Data model

```mermaid
erDiagram
    dim_date ||--o{ fact_transactions : date_key
    dim_users ||--o{ fact_transactions : user_key
    dim_merchants ||--o{ fact_transactions : merchant_key
    dim_location ||--o{ fact_transactions : location_key
    dim_merchant_category ||--o{ dim_merchants : mcc
    fact_transactions ||--o| fact_fraud_alerts : transaction_id
    fact_transactions ||--o{ fact_chargebacks : transaction_id
    dim_dispute_reason ||--o{ fact_chargebacks : reason_code
    dim_merchants ||--o{ agg_merchant_window_risk : merchant_key
    dim_merchants ||--o{ agg_hourly_merchant_risk : merchant_key

    fact_transactions {
        uuid transaction_id PK
        timestamptz event_ts PK "daily range partitions"
        numeric amount_usd
        real implied_speed_kmh "stateful feature"
        smallint txn_count_5m "stateful feature"
        real amount_zscore "stateful feature"
        numeric fraud_score
        text_array reason_codes
        timestamptz kafka_ts "lineage"
        timestamptz loaded_at "latency SLO"
    }
    dim_users {
        bigint user_key PK
        text user_id "natural key"
        text card_tier "SCD2 tracked"
        timestamptz valid_from
        timestamptz valid_to
        boolean is_current
    }
    fact_fraud_alerts {
        bigint alert_id PK
        uuid transaction_id UK
        text status "OPEN / CONFIRMED_FRAUD / FALSE_POSITIVE"
    }
    agg_merchant_window_risk {
        bigint merchant_key PK
        timestamptz window_start PK
        int txn_count
        int flagged_count
    }
```

Full DDL: [`sql/init_schema.sql`](sql/init_schema.sql).

---

## Analytical SQL library

[`sql/analytics_kpis.sql`](sql/analytics_kpis.sql). Every model is executed against a seeded warehouse
in CI.

| Model | Business question | Techniques |
|---|---|---|
| `executive_kpi_snapshot` | Today vs. the previous 24 h | `FILTER` aggregates, LATERAL, period-over-period deltas |
| `trailing_30d_customer_fraud_rate` | Cardholders with the highest trailing fraud rate | `RANGE BETWEEN INTERVAL '29 days' PRECEDING` window frame |
| `riskiest_merchant_categories` | Top 5 categories by chargeback probability, robust to small samples | empirical-Bayes shrinkage, Wilson lower bound, `RANK()` |
| `velocity_anomalies_consecutive_swipes` | Physically impossible consecutive swipes | `LAG()`/`LEAD()`, haversine UDF |
| `rule_effectiveness` | Precision, recall, F1 and lift per rule | `UNNEST` + `LATERAL`, confusion-matrix algebra |
| `alert_threshold_tuning_curve` | Precision/recall/FPR if the threshold moves | `generate_series` parameter sweep |
| `fraud_incidents_time_to_detect` | Fraud dollars lost before the first alert | gaps-and-islands, percentiles |
| `merchant_hourly_spike_zscores` | Merchants far outside their own hourly norm | trailing `AVG`/`STDDEV` window frames |
| `sliding_window_merchant_risk_trend` | Is a high-risk merchant's flagged share accelerating? | `LAG`, moving average, `FIRST_VALUE` |
| `customer_spend_deciles` | Is fraud concentrated in heavy or light spenders? | `NTILE(10)`, cumulative share |
| `geography_rollup` | Volume and risk with subtotals | `GROUP BY ROLLUP`, `GROUPING()` |
| `chargeback_latency_by_reason` | How late do labels arrive? | `percentile_cont` per group |
| `weekday_hour_risk_heatmap` | When is fraud pressure highest? | conformed date dimension |
| `scd2_card_tier_history` | Behaviour under each card-tier version | SCD2 point-in-time join |
| `pipeline_latency_slo` | Is each query meeting the 2 s micro-batch SLO? | `date_bin`, SLO attainment ratio |

```bash
make kpis        # or: docker compose exec -T postgres psql -U fraud -d fraud_dw -f - < sql/analytics_kpis.sql
```

## Dashboard

`http://localhost:8501`, auto-refreshing every 5 s:

* **KPI tiles**: transactions, ingest throughput, alert rate, confirmed fraud, p95 end-to-end latency
  (Kafka append → queryable in Postgres) and p95 micro-batch duration.
* Transactions and alert rate over time (separate charts, never a dual axis).
* Live alert feed with risk level, score and the rules that fired.
* World map of flagged transactions by risk level.
* Merchant-category × time heatmap of alert rate.
* Rule precision/recall against chargeback labels, plus overall precision, recall and false-positive rate.
* Merchants spiking against their own sliding-window baseline.
* Pipeline health per streaming query: batch latency percentiles, rows/s, Kafka lag, watermark,
  state size and late rows dropped.

The queries behind every panel live in [`dashboard/queries.py`](dashboard/queries.py) and are executed
by the integration tests.

---

## Quick start

**Prerequisites:** Docker with Compose v2.24+, ~10 GB free disk, and **8 GB of RAM available to
Docker** for the full cluster (see *laptop mode* below for smaller machines).

```bash
cp .env.example .env                  # optional: tune rates, population, credentials
docker compose up -d --build          # builds images, seeds dimensions, starts everything
docker compose logs -f pipeline       # wait for "layer gold started"
```

Then open the dashboard at <http://localhost:8501>, the Spark cluster at <http://localhost:8080>
and the streaming queries at <http://localhost:4040> (Structured Streaming tab).

Useful commands (a `Makefile` wraps them if you have `make`):

```bash
PRODUCER_RATE=5000 docker compose up -d --no-deps --force-recreate producer   # change the event rate
docker compose exec -T postgres psql -U fraud -d fraud_dw -f - < scripts/benchmark.sql  # latency/throughput report
docker compose --profile batch run --rm reconcile                             # nightly reconciliation job
docker compose run --rm seed python -m producer.seed_dimensions --simulate-upgrades 500  # SCD2 demo
docker compose exec postgres psql -U fraud -d fraud_dw                        # explore the warehouse
docker compose down -v                                                        # stop and delete all data
```

### Laptop mode

On machines with less than ~16 GB of RAM, running three extra Spark JVMs can push the Docker VM
into host swap. Laptop mode runs the identical job with Spark in local mode inside the driver
container (same queries, same checkpoints):

```bash
docker compose -f docker-compose.yml -f docker-compose.laptop.yml up -d --build
```

### Configuration

All settings are environment variables ([`common/settings.py`](common/settings.py)); the most useful:

| Variable | Default | Meaning |
|---|---|---|
| `PRODUCER_RATE` / `PRODUCER_WORKERS` | `1000` / `1` | new transactions per second / producer processes |
| `SIM_USERS` / `SIM_MERCHANTS` / `SIM_SEED` | `100000` / `2500` / `42` | synthetic population (deterministic per seed) |
| `SIM_FRAUD_SCENARIO_RATE` | `0.0006` | chance each event also launches a fraud scenario |
| `STREAM_TRIGGER_INTERVAL` | `2 seconds` | micro-batch trigger for the latency-critical path |
| `STREAM_WATERMARK_DELAY` | `10 minutes` | lateness tolerated before events are dropped |
| `STREAM_WINDOW_DURATION` / `STREAM_WINDOW_SLIDE` | `5 minutes` / `1 minute` | merchant sliding windows |
| `STREAM_MAX_OFFSETS_PER_TRIGGER` | `50000` | backpressure: cap on records per bronze batch |
| `FRAUD_RULES_PATH` | `config/fraud_rules.yaml` | rule weights and thresholds |

---

## Testing

| Suite | What it covers | Where it runs |
|---|---|---|
| `tests/test_features.py` | Stateful feature engine: trailing windows, out-of-order events, retention, Welford z-scores, EWMA baselines, state round-trip | pure Python |
| `tests/test_producer.py` | Population determinism, merchant coverage, fraud scenarios and chargeback labels, chaos injection | pure Python |
| `tests/test_schemas.py` | Producer ↔ Spark ↔ feature-engine contracts; parsing and every quarantine reason | Spark |
| `tests/test_streaming_transforms.py` | **Real micro-batches** through file source → memory sink: cross-batch dedup, sliding windows finalised by the watermark with late data dropped (and counted), state carried across batches by `applyInPandasWithState`, enrichment with unknown members | Spark |
| `tests/test_scoring.py` | Rule config validation; each rule, noisy-OR combination, risk bands, NULL-safety | Spark |
| `tests/test_pipeline_utils.py` | Upsert SQL generation, duration parsing, progress-metric extraction | Python + PySpark |
| `tests/integration/test_sql_models.py` | Schema idempotency, every analytics model and dashboard query, SCD2 merge, partition maintenance, idempotent upsert writer | PostgreSQL |

```bash
docker compose --profile test run --rm tests                  # unit + Spark tests inside the Spark image
# or locally (Java 17/21 + Python 3.10-3.13):
pip install -r requirements-dev.txt && pytest -m "not integration"
TEST_POSTGRES_DSN=postgresql://fraud:fraud@localhost:5432/postgres pytest -m integration
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs ruff, the unit/Spark suite on JDK 17,
the SQL suite against a Postgres service container, and builds both images.

Two real bugs were caught this way during development: Spark's `least()` skips NULLs, which turned
an unknown home location into a 20,015 km "distance from home" (`expressions.haversine_km`), and
`trailing` is a reserved word in PostgreSQL.

---

## Benchmarks

The pipeline records its own performance: a `StreamingQueryListener` writes every micro-batch's
duration, input/processing rate, watermark, state size, late rows dropped and Kafka lag to
`ops.streaming_query_progress`, and every warehouse row carries `kafka_ts` and `loaded_at`, so
end-to-end latency is measured, not estimated.

```bash
# 1. drive load (one producer process sustains ~10k events/s; add workers for more)
PRODUCER_RATE=10000 PRODUCER_WORKERS=2 docker compose up -d --no-deps --force-recreate producer
# 2. after a few minutes of steady state, print the report
docker compose exec -T postgres psql -U fraud -d fraud_dw -f - < scripts/benchmark.sql
```

The report ([`scripts/benchmark.sql`](scripts/benchmark.sql)) shows, per streaming query: batches,
rows processed, p50/p95/max micro-batch duration, processing rows/s, % of batches under the 2 s SLO,
max Kafka lag and late rows dropped; plus per-minute ingest throughput and p50/p95/p99 end-to-end
latency (Kafka append → queryable in Postgres). The same numbers are live on the dashboard's
*Pipeline health* panel and in the `pipeline_latency_slo` SQL model.

Throughput scales with executor cores. The main tuning levers are `STREAM_TRIGGER_INTERVAL`,
`STREAM_MAX_OFFSETS_PER_TRIGGER` (backpressure), the number of Kafka partitions (bronze read
parallelism) and `spark.sql.shuffle.partitions` (parallelism of the stateful operators; fixed
per checkpoint). On a constrained laptop, use laptop mode and a lower `PRODUCER_RATE`.

---

## Project layout

```
├── docker-compose.yml            full stack: Kafka (KRaft), Postgres, Spark master + 2 workers, jobs, UI
├── docker-compose.laptop.yml     same job in Spark local mode for small machines
├── docker/
│   ├── spark/                    Spark 4.1.3 image: connector JARs resolved at build time, Python deps
│   └── app/                      slim Python image: producer, seeder, dashboard
├── config/
│   ├── spark-defaults.conf       Delta, RocksDB state store, FAIR pools, sizing
│   ├── fairscheduler.xml         one scheduler pool per medallion layer
│   └── fraud_rules.yaml          rule weights and thresholds
├── common/                       settings, reference data (cities, MCCs, FX, dispute codes), geo
├── producer/
│   ├── entities.py               deterministic cardholder / merchant population
│   ├── simulator.py              traffic, fraud scenarios, chargebacks, chaos
│   ├── transaction_producer.py   multi-process idempotent Kafka producer
│   └── seed_dimensions.py        reference + SCD2 dimension loader
├── streaming/
│   ├── schemas.py                explicit contracts for every layer
│   ├── bronze_ingestion.py       Kafka -> Delta
│   ├── silver_transforms.py      parse, DQ/quarantine, dedup, sliding windows
│   ├── features.py               per-card stateful feature engine
│   ├── scoring.py                rule engine
│   ├── gold_sink.py              enrichment, scoring, Delta + Postgres sinks, merchant windows
│   ├── postgres_writer.py        parallel COPY + ON CONFLICT upserts
│   ├── monitoring.py             StreamingQueryListener -> ops tables
│   └── run_pipeline.py           entry point
├── batch/historical_reconciliation.py
├── dashboard/                    Streamlit app + its SQL
├── sql/                          warehouse DDL + analytical model library
├── scripts/benchmark.sql         latency / throughput report from pipeline telemetry
└── tests/                        unit, Spark streaming and PostgreSQL integration tests
```

## Limitations and next steps

* **Single-node everything.** Kafka and Postgres run one node each; a real deployment would use a
  replicated Kafka cluster, object storage (S3/ADLS) for the lakehouse (paths are URIs, so this is
  configuration plus `hadoop-aws`), and a managed Postgres or ClickHouse for serving.
* **Rules, not a model.** The rule engine is deliberately explainable. The gold table already holds
  labelled features (via chargebacks), which makes it a ready training set for a gradient-boosted
  model served next to the rules.
* **State eviction** is event-time based (1 hour idle). Long-horizon behavioural profiles would move
  to a feature store refreshed by the batch layer.
* `transformWithStateInPandas` (Spark 4) would allow list/map state with TTL instead of one pickled
  state object per card.

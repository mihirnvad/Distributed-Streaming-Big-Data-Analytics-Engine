"""Gold layer: stateful features -> enrichment -> scoring -> lakehouse + warehouse.

silver.transactions
   |  applyInPandasWithState(user_id)      per-card trailing windows, travel speed, z-scores
   v
foreachBatch (one micro-batch)
   |  broadcast-join cached dimensions     user/merchant/location surrogate keys, home geo
   |  rule engine                          fraud_score, risk_level, reason_codes
   |-> gold.scored_transactions (Delta)    idempotent via txnAppId/txnVersion
   |-> dw.fact_transactions   (Postgres)   COPY + INSERT .. ON CONFLICT DO NOTHING
   '-> dw.fact_fraud_alerts   (Postgres)   flagged rows only

gold.scored_transactions (Delta stream)
   '-> 5-min/1-min sliding windows per merchant (UPDATE mode) -> dw.agg_merchant_window_risk upsert

silver.chargebacks
   '-> dw.fact_chargebacks + alert status CONFIRMED_FRAUD
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import dataclass

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.streaming.state import GroupStateTimeout

from common.settings import PostgresSettings, Settings
from streaming.expressions import date_key, haversine_km
from streaming.features import make_state_function
from streaming.postgres_writer import UpsertSpec, upsert_dataframe
from streaming.schemas import (
    GOLD_TRANSACTION_SCHEMA,
    USER_FEATURE_OUTPUT_SCHEMA,
    USER_FEATURE_STATE_SCHEMA,
)
from streaming.scoring import RuleSet, apply_rules, load_rules
from streaming.silver_transforms import sliding_window_aggregate
from streaming.spark_session import ensure_delta_table, scheduler_pool, with_trigger

log = logging.getLogger(__name__)

GOLD_COLUMNS = [f.name for f in GOLD_TRANSACTION_SCHEMA.fields]

FACT_TRANSACTIONS = UpsertSpec(
    table="dw.fact_transactions",
    columns=(
        "transaction_id", "event_ts", "date_key", "user_key", "merchant_key", "location_key", "amount",
        "currency_code", "amount_usd", "channel", "entry_mode", "txn_lat", "txn_lon", "distance_from_home_km",
        "km_from_prev", "seconds_since_prev", "implied_speed_kmh", "txn_count_1m", "txn_count_5m",
        "amount_usd_5m", "distinct_merchants_5m", "small_txn_count_5m", "amount_zscore", "fraud_score",
        "risk_level", "is_flagged", "reason_codes", "kafka_partition", "kafka_offset", "kafka_ts", "processed_at",
    ),
    conflict_columns=("transaction_id", "event_ts"),
)  # fmt: skip

FACT_FRAUD_ALERTS = UpsertSpec(
    table="dw.fact_fraud_alerts",
    columns=(
        "transaction_id", "event_ts", "date_key", "user_key", "merchant_key", "amount_usd", "fraud_score",
        "risk_level", "reason_codes",
    ),
    conflict_columns=("transaction_id",),
)  # fmt: skip

AGG_MERCHANT_WINDOWS = UpsertSpec(
    table="dw.agg_merchant_window_risk",
    columns=(
        "merchant_key", "window_start", "window_end", "txn_count", "total_amount_usd", "max_amount_usd",
        "approx_distinct_users", "flagged_count", "avg_fraud_score",
    ),
    conflict_columns=("merchant_key", "window_start"),
    update_columns=(
        "window_end", "txn_count", "total_amount_usd", "max_amount_usd", "approx_distinct_users",
        "flagged_count", "avg_fraud_score",
    ),
    touch_column="updated_at",
)  # fmt: skip

FACT_CHARGEBACKS = UpsertSpec(
    table="dw.fact_chargebacks",
    columns=(
        "chargeback_id", "transaction_id", "user_key", "reason_code", "amount", "currency_code", "amount_usd",
        "reported_ts",
    ),
    conflict_columns=("chargeback_id",),
    post_statements=(
        # A fraud-coded dispute confirms the alert on that transaction.
        """
        UPDATE dw.fact_fraud_alerts a
           SET status = 'CONFIRMED_FRAUD', status_updated_at = now()
          FROM {staging} s
          JOIN dw.dim_dispute_reason r ON r.reason_code = s.reason_code AND r.is_fraud
         WHERE a.transaction_id = s.transaction_id
           AND a.status <> 'CONFIRMED_FRAUD'
        """,
    ),
)  # fmt: skip


_DURATION = re.compile(r"^\s*(\d+)\s*(ms|milliseconds?|s|secs?|seconds?|m|mins?|minutes?|h|hours?)\s*$", re.I)
_UNIT_MS = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000}


def parse_duration_ms(text: str) -> int:
    """'10 minutes' -> 600000. Accepts ms/s/m/h and their long forms."""
    match = _DURATION.match(text)
    if not match:
        raise ValueError(f"cannot parse duration: {text!r}")
    value, unit = int(match.group(1)), match.group(2).lower()
    key = "ms" if unit.startswith("ms") or unit.startswith("milli") else unit[0]
    return value * _UNIT_MS[key]


# --------------------------------------------------------------------------- dimensions


@dataclass
class Dimensions:
    users: DataFrame
    merchants: DataFrame
    locations: DataFrame
    loaded_at: float

    def unpersist(self) -> None:
        for df in (self.users, self.merchants, self.locations):
            df.unpersist()


class DimensionCache:
    """Current-version dimension snapshots from Postgres, cached and periodically refreshed.

    Dimensions change slowly, so re-reading them every micro-batch would waste a JDBC
    round-trip per batch; instead they are cached in executor memory and reloaded
    every ``refresh_seconds`` (the staleness SLA for new cards/merchants).
    """

    def __init__(self, spark: SparkSession, pg: PostgresSettings, refresh_seconds: int) -> None:
        self.spark = spark
        self.pg = pg
        self.refresh_seconds = refresh_seconds
        self._current: Dimensions | None = None
        self._lock = threading.Lock()

    def _jdbc(self, query: str) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .option("url", self.pg.jdbc_url)
            .option("query", query)
            .option("user", self.pg.user)
            .option("password", self.pg.password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", 10_000)
            .load()
        )

    def _load(self) -> Dimensions:
        users = self._jdbc(
            "SELECT user_key, user_id, home_lat, home_lon, account_open_date, has_chip_card "
            "FROM dw.dim_users WHERE is_current AND user_key > 0"
        )
        merchants = self._jdbc(
            "SELECT m.merchant_key, m.merchant_id, m.mcc::text AS mcc, c.risk_tier AS category_risk_tier "
            "FROM dw.dim_merchants m JOIN dw.dim_merchant_category c ON c.mcc = m.mcc WHERE m.merchant_key > 0"
        )
        locations = self._jdbc(
            "SELECT location_key, city, country_code::text AS country_code FROM dw.dim_location WHERE location_key > 0"
        )
        dims = Dimensions(
            users=users.persist(StorageLevel.MEMORY_AND_DISK),
            merchants=merchants.persist(StorageLevel.MEMORY_AND_DISK),
            locations=locations.persist(StorageLevel.MEMORY_AND_DISK),
            loaded_at=time.monotonic(),
        )
        counts = (dims.users.count(), dims.merchants.count(), dims.locations.count())
        log.info("dimension cache loaded: users=%d merchants=%d locations=%d", *counts)
        return dims

    def get(self) -> Dimensions:
        with self._lock:
            stale = self._current is None or time.monotonic() - self._current.loaded_at > self.refresh_seconds
            if stale:
                previous, self._current = self._current, self._load()
                if previous is not None:
                    previous.unpersist()
            assert self._current is not None
            return self._current


def enrich(df: DataFrame, dims: Dimensions) -> DataFrame:
    """Resolve surrogate keys and derive dimension-dependent features.

    Unknown natural keys map to the -1 "unknown member" rows instead of dropping the
    fact, so a card created seconds ago is still scored and loaded.
    """
    joined = (
        df.join(F.broadcast(dims.users), "user_id", "left")
        .join(F.broadcast(dims.merchants), "merchant_id", "left")
        .join(F.broadcast(dims.locations), ["city", "country_code"], "left")
    )
    return joined.select(
        *[F.col(c) for c in df.columns],
        F.coalesce(F.col("user_key"), F.lit(-1)).cast("long").alias("user_key"),
        F.coalesce(F.col("merchant_key"), F.lit(-1)).cast("long").alias("merchant_key"),
        F.coalesce(F.col("location_key"), F.lit(-1)).cast("int").alias("location_key"),
        date_key(F.col("event_ts")).alias("date_key"),
        F.coalesce(F.col("mcc"), F.lit("0000")).alias("mcc"),
        F.coalesce(F.col("category_risk_tier"), F.lit("UNKNOWN")).alias("category_risk_tier"),
        F.coalesce(F.col("has_chip_card"), F.lit(True)).alias("has_chip_card"),
        F.datediff(F.col("event_date"), F.col("account_open_date")).alias("account_age_days"),
        haversine_km(F.col("home_lat"), F.col("home_lon"), F.col("location_lat"), F.col("location_lon")).alias(
            "distance_from_home_km"
        ),
    )


def score_batch(df: DataFrame, dims: Dimensions, rules: RuleSet) -> DataFrame:
    scored = apply_rules(enrich(df, dims), rules).withColumn("processed_at", F.current_timestamp())
    return scored.select(*GOLD_COLUMNS)


# --------------------------------------------------------------------------- sinks


class GoldTransactionsSink:
    """foreachBatch handler that fans one scored micro-batch out to every gold sink."""

    def __init__(self, settings: Settings, dims: DimensionCache, rules: RuleSet, checkpoint: str) -> None:
        self.settings = settings
        self.dims = dims
        self.rules = rules
        self.checkpoint = checkpoint
        self._app_id: str | None = None

    def _delta_app_id(self, spark: SparkSession) -> str:
        """Idempotency key for Delta writes: the streaming query id stored in the checkpoint.

        Using the query id (not a constant) means a deliberately reset checkpoint starts a
        fresh id, so Delta does not mistake new batch 0 for an already-committed one.
        """
        if self._app_id is None:
            meta = spark.read.text(f"{self.checkpoint}/metadata").first()
            self._app_id = "gold_transactions-" + json.loads(meta[0])["id"]
        return self._app_id

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession
        started = time.perf_counter()
        scored = score_batch(batch_df, self.dims.get(), self.rules).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            rows = scored.count()
            if rows == 0:
                return
            (
                scored.write.format("delta")
                .mode("append")
                .option("txnAppId", self._delta_app_id(spark))
                .option("txnVersion", batch_id)
                .partitionBy("event_date")
                .save(self.settings.lakehouse.gold_transactions)
            )
            dsn = self.settings.postgres.dsn
            parts = self.settings.streaming.postgres_write_partitions
            facts = scored.select(
                "*",
                F.col("currency").alias("currency_code"),
                F.col("location_lat").alias("txn_lat"),
                F.col("location_lon").alias("txn_lon"),
            )
            upsert_dataframe(facts, FACT_TRANSACTIONS, dsn, parts)
            upsert_dataframe(scored.where("is_flagged"), FACT_FRAUD_ALERTS, dsn, parts)
            log.info("gold batch %d: %d rows in %.2fs", batch_id, rows, time.perf_counter() - started)
        finally:
            scored.unpersist()


def _merchant_windows_sink(settings: Settings):
    dsn = settings.postgres.dsn

    def write(batch_df: DataFrame, batch_id: int) -> None:
        upsert_dataframe(batch_df, AGG_MERCHANT_WINDOWS, dsn, settings.streaming.postgres_write_partitions)

    return write


def _chargebacks_sink(settings: Settings, dims: DimensionCache):
    dsn = settings.postgres.dsn

    def write(batch_df: DataFrame, batch_id: int) -> None:
        users = dims.get().users.select("user_id", "user_key")
        enriched = batch_df.join(F.broadcast(users), "user_id", "left").select(
            "chargeback_id",
            "transaction_id",
            F.coalesce(F.col("user_key"), F.lit(-1)).cast("long").alias("user_key"),
            "reason_code",
            "amount",
            F.col("currency").alias("currency_code"),
            "amount_usd",
            "reported_ts",
        )
        upsert_dataframe(enriched, FACT_CHARGEBACKS, dsn, 1)

    return write


# --------------------------------------------------------------------------- wiring


def user_feature_stream(silver: DataFrame, watermark: str, idle_timeout: str) -> DataFrame:
    """Per-card stateful feature computation (see streaming/features.py)."""
    return (
        silver.withWatermark("event_ts", watermark)
        .groupBy("user_id")
        .applyInPandasWithState(
            make_state_function(parse_duration_ms(idle_timeout)),
            outputStructType=USER_FEATURE_OUTPUT_SCHEMA,
            stateStructType=USER_FEATURE_STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
    )


def merchant_risk_windows(gold: DataFrame, window: str, slide: str, watermark: str) -> DataFrame:
    windows = sliding_window_aggregate(
        gold,
        keys=["merchant_key"],
        event_time="event_ts",
        window_duration=window,
        slide_duration=slide,
        watermark=watermark,
        aggregations={
            "txn_count": F.count(F.lit(1)),
            "total_amount_usd": F.sum("amount_usd"),
            "max_amount_usd": F.max("amount_usd"),
            "approx_distinct_users": F.approx_count_distinct("user_key"),
            "flagged_count": F.sum(F.col("is_flagged").cast("int")),
            "avg_fraud_score": F.avg("fraud_score"),
        },
    )
    return windows.select(
        "merchant_key",
        "window_start",
        "window_end",
        F.col("txn_count").cast("int"),
        F.col("total_amount_usd").cast("decimal(16,2)"),
        F.col("max_amount_usd").cast("decimal(14,2)"),
        F.col("approx_distinct_users").cast("int"),
        F.col("flagged_count").cast("int"),
        F.round("avg_fraud_score", 4).cast("decimal(6,4)").alias("avg_fraud_score"),
    )


def start(spark: SparkSession, settings: Settings) -> list[StreamingQuery]:
    lake, s = settings.lakehouse, settings.streaming
    rules = load_rules(s.rules_path)
    log.info("loaded rule set %s with %d active rules", rules.version, len(rules.active_rules))
    dims = DimensionCache(spark, settings.postgres, s.dimension_refresh_seconds)
    ensure_delta_table(spark, lake.gold_transactions, GOLD_TRANSACTION_SCHEMA, ["event_date"])

    queries: list[StreamingQuery] = []
    with scheduler_pool(spark, "gold"):
        silver = spark.readStream.format("delta").load(lake.silver_transactions)
        features = user_feature_stream(silver, s.watermark_delay, s.user_state_idle_timeout)
        checkpoint = lake.checkpoint("gold_transactions")
        queries.append(
            with_trigger(
                features.writeStream.queryName("gold_transactions")
                .outputMode("append")
                .foreachBatch(GoldTransactionsSink(settings, dims, rules, checkpoint))
                .option("checkpointLocation", checkpoint),
                s.trigger_interval,
            ).start()
        )

    with scheduler_pool(spark, "gold_aggregates"):
        gold = spark.readStream.format("delta").load(lake.gold_transactions)
        queries.append(
            with_trigger(
                merchant_risk_windows(gold, s.window_duration, s.window_slide, s.watermark_delay)
                .writeStream.queryName("gold_merchant_windows")
                .outputMode("update")
                .foreachBatch(_merchant_windows_sink(settings))
                .option("checkpointLocation", lake.checkpoint("gold_merchant_windows")),
                s.slow_trigger_interval,
            ).start()
        )
        chargebacks = spark.readStream.format("delta").load(lake.silver_chargebacks)
        queries.append(
            with_trigger(
                chargebacks.writeStream.queryName("gold_chargebacks")
                .outputMode("append")
                .foreachBatch(_chargebacks_sink(settings, dims))
                .option("checkpointLocation", lake.checkpoint("gold_chargebacks")),
                s.slow_trigger_interval,
            ).start()
        )
    return queries

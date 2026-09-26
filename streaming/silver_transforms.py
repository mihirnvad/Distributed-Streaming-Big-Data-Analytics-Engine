"""Silver layer: parse, validate, quarantine, normalise, deduplicate.

bronze.transactions --parse/validate--+--> valid ---watermark + dedupe--> silver.transactions
                                      +--> invalid ---------------------> silver.quarantine

* Parsing uses the explicit schema contract; malformed JSON is captured via the
  corrupt-record column instead of failing the stream.
* Every failed data-quality rule is recorded as a reason code, so quarantine
  doubles as a data-quality report.
* Deduplication uses ``dropDuplicatesWithinWatermark``: duplicates are removed
  across micro-batches while state is bounded by the event-time watermark
  (plain ``dropDuplicates`` on a stream keeps state forever).

The transformation functions are pure ``DataFrame -> DataFrame`` so they work
identically on streaming and batch DataFrames, which is how they are unit tested.
"""

from __future__ import annotations

import functools
import operator

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common.reference import CHANNELS, DISPUTE_REASONS
from common.settings import Settings
from streaming.expressions import to_usd, usd_rate
from streaming.schemas import (
    CHARGEBACK_EVENT_SCHEMA,
    CORRUPT_RECORD_COLUMN,
    QUARANTINE_SCHEMA,
    SILVER_CHARGEBACK_SCHEMA,
    SILVER_TRANSACTION_SCHEMA,
    SUPPORTED_SCHEMA_VERSIONS,
    TRANSACTION_EVENT_SCHEMA,
    with_corrupt_record,
)
from streaming.spark_session import ensure_delta_table, read_delta_stream, scheduler_pool, with_trigger

REQUIRED_TRANSACTION_FIELDS = ("transaction_id", "event_ts", "user_id", "merchant_id", "amount", "currency")
REQUIRED_CHARGEBACK_FIELDS = ("chargeback_id", "transaction_id", "user_id", "reason_code", "amount", "currency")
MAX_AMOUNT = 1_000_000  # anything above is a unit/overflow bug, not a purchase
CLOCK_SKEW_TOLERANCE = "INTERVAL 5 MINUTES"
_JSON_OPTIONS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN}


def _check(condition: Column, code: str) -> Column:
    return F.when(condition, F.lit(code))


def _any_null(fields: tuple[str, ...]) -> Column:
    return functools.reduce(operator.or_, [F.col(f).isNull() for f in fields])


def _parse_payload(bronze: DataFrame, schema) -> DataFrame:
    """Parse each JSON payload exactly once and expand its fields into columns.

    A plain ``select(from_json(...).alias("j")).select("j.*")`` lets the optimizer
    inline the ``from_json`` call into every field reference and pushed-down filter,
    re-parsing each payload ~20 times. ``inline(array(...))`` is a generator, which the
    optimizer cannot inline through, so parsing happens once per record.
    """
    parsed = F.from_json("raw_payload", with_corrupt_record(schema), _JSON_OPTIONS)
    return bronze.select(
        "raw_payload", "topic", "kafka_partition", "kafka_offset", "kafka_ts", F.inline(F.array(parsed))
    )


# --------------------------------------------------------------------------- transactions


def parse_transactions(bronze: DataFrame) -> DataFrame:
    """Parse raw payloads and attach a ``dq_errors`` array (empty = valid)."""
    parsed = _parse_payload(bronze, TRANSACTION_EVENT_SCHEMA)
    parsed = parsed.withColumn("event_ts_parsed", F.try_to_timestamp(F.col("event_ts")))
    lat, lon = F.col("location_lat"), F.col("location_lon")
    errors = F.array_compact(
        F.array(
            _check(F.col(CORRUPT_RECORD_COLUMN).isNotNull(), "MALFORMED_JSON"),
            _check(
                F.col("schema_version").isNull() | ~F.col("schema_version").isin(*SUPPORTED_SCHEMA_VERSIONS),
                "UNSUPPORTED_SCHEMA_VERSION",
            ),
            _check(_any_null(REQUIRED_TRANSACTION_FIELDS), "MISSING_REQUIRED_FIELD"),
            _check(F.col("event_ts").isNotNull() & F.col("event_ts_parsed").isNull(), "INVALID_TIMESTAMP"),
            _check(F.col("event_ts_parsed") > F.col("kafka_ts") + F.expr(CLOCK_SKEW_TOLERANCE), "FUTURE_TIMESTAMP"),
            _check((F.col("amount") <= 0) | (F.col("amount") > MAX_AMOUNT), "INVALID_AMOUNT"),
            _check(F.col("currency").isNotNull() & usd_rate(F.col("currency")).isNull(), "UNKNOWN_CURRENCY"),
            _check(~lat.between(-90, 90) | ~lon.between(-180, 180), "INVALID_COORDINATES"),
            _check(F.col("channel").isNotNull() & ~F.col("channel").isin(*CHANNELS), "INVALID_CHANNEL"),
        )
    )
    return parsed.withColumn("dq_errors", errors)


def valid_transactions(parsed: DataFrame) -> DataFrame:
    """Rows that passed every check, normalised onto the silver contract."""
    return parsed.where(F.size("dq_errors") == 0).select(
        "transaction_id",
        F.col("event_ts_parsed").alias("event_ts"),
        F.to_date("event_ts_parsed").alias("event_date"),
        "user_id",
        "card_id",
        "merchant_id",
        "amount",
        F.upper("currency").alias("currency"),
        to_usd(F.col("amount"), F.upper("currency")).alias("amount_usd"),
        "channel",
        "entry_mode",
        "location_lat",
        "location_lon",
        "city",
        "country_code",
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
    )


def quarantined_records(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.size("dq_errors") > 0).select(
        "raw_payload",
        "dq_errors",
        "topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        F.current_timestamp().alias("quarantined_at"),
        F.current_date().alias("quarantine_date"),
    )


def deduplicate(df: DataFrame, key: str, event_time: str, watermark: str) -> DataFrame:
    """Drop replays of the same business key seen within the watermark horizon."""
    return df.withWatermark(event_time, watermark).dropDuplicatesWithinWatermark([key])


# --------------------------------------------------------------------------- chargebacks


def parse_chargebacks(bronze: DataFrame) -> DataFrame:
    parsed = _parse_payload(bronze, CHARGEBACK_EVENT_SCHEMA)
    parsed = parsed.withColumn("reported_ts_parsed", F.try_to_timestamp(F.col("reported_ts")))
    known_codes = [r.code for r in DISPUTE_REASONS]
    errors = F.array_compact(
        F.array(
            _check(F.col(CORRUPT_RECORD_COLUMN).isNotNull(), "MALFORMED_JSON"),
            _check(_any_null(REQUIRED_CHARGEBACK_FIELDS), "MISSING_REQUIRED_FIELD"),
            _check(F.col("reported_ts_parsed").isNull(), "INVALID_TIMESTAMP"),
            _check(~F.col("reason_code").isin(*known_codes), "UNKNOWN_REASON_CODE"),
            _check(F.col("amount") <= 0, "INVALID_AMOUNT"),
            _check(usd_rate(F.col("currency")).isNull(), "UNKNOWN_CURRENCY"),
        )
    )
    return parsed.withColumn("dq_errors", errors)


def valid_chargebacks(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.size("dq_errors") == 0).select(
        "chargeback_id",
        "transaction_id",
        "user_id",
        "reason_code",
        "amount",
        "currency",
        to_usd(F.col("amount"), F.col("currency")).alias("amount_usd"),
        F.col("reported_ts_parsed").alias("reported_ts"),
        "kafka_ts",
    )


# --------------------------------------------------------------------------- windowed aggregation


def sliding_window_aggregate(
    df: DataFrame,
    keys: list[str],
    event_time: str,
    window_duration: str,
    slide_duration: str,
    watermark: str,
    aggregations: dict[str, Column],
) -> DataFrame:
    """Watermarked sliding-window aggregation.

    Each event contributes to ``window_duration / slide_duration`` overlapping
    windows. The watermark bounds how long a window's state is kept: once
    ``max(event_time) - watermark`` passes a window's end, the window is final
    (append mode emits it; update mode stops updating it) and its state is dropped.
    Events older than the watermark are discarded and counted in the query's
    ``numRowsDroppedByWatermark`` metric.
    """
    return (
        df.withWatermark(event_time, watermark)
        .groupBy(F.window(event_time, window_duration, slide_duration).alias("w"), *keys)
        .agg(*[column.alias(name) for name, column in aggregations.items()])
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            *keys,
            *aggregations.keys(),
        )
    )


# --------------------------------------------------------------------------- wiring


def start(spark: SparkSession, settings: Settings) -> list[StreamingQuery]:
    lake, s = settings.lakehouse, settings.streaming
    ensure_delta_table(spark, lake.silver_transactions, SILVER_TRANSACTION_SCHEMA, ["event_date"])
    ensure_delta_table(spark, lake.silver_chargebacks, SILVER_CHARGEBACK_SCHEMA, [])
    ensure_delta_table(spark, lake.silver_quarantine, QUARANTINE_SCHEMA, ["quarantine_date"])

    queries: list[StreamingQuery] = []
    with scheduler_pool(spark, "silver"):
        bronze_txn = parse_transactions(read_delta_stream(spark, lake.bronze_transactions, s.max_bytes_per_trigger))

        clean = deduplicate(valid_transactions(bronze_txn), "transaction_id", "event_ts", s.watermark_delay)
        queries.append(
            with_trigger(
                clean.writeStream.queryName("silver_transactions")
                .format("delta")
                .outputMode("append")
                .partitionBy("event_date")
                .option("checkpointLocation", lake.checkpoint("silver_transactions")),
                s.trigger_interval,
            ).start(lake.silver_transactions)
        )

        queries.append(
            with_trigger(
                quarantined_records(bronze_txn)
                .writeStream.queryName("silver_quarantine")
                .format("delta")
                .outputMode("append")
                .partitionBy("quarantine_date")
                .option("checkpointLocation", lake.checkpoint("silver_quarantine")),
                s.slow_trigger_interval,
            ).start(lake.silver_quarantine)
        )

        chargebacks = deduplicate(
            valid_chargebacks(
                parse_chargebacks(read_delta_stream(spark, lake.bronze_chargebacks, s.max_bytes_per_trigger))
            ),
            "chargeback_id",
            "reported_ts",
            s.watermark_delay,
        )
        queries.append(
            with_trigger(
                chargebacks.writeStream.queryName("silver_chargebacks")
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", lake.checkpoint("silver_chargebacks")),
                s.slow_trigger_interval,
            ).start(lake.silver_chargebacks)
        )
    return queries

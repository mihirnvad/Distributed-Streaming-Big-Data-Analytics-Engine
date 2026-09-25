"""Explicit Spark schemas for every layer.

Schemas are declared, never inferred: inference on a stream is impossible to do
safely (it would need to sample future data) and silently widens types when a
producer misbehaves. A declared contract turns producer drift into quarantined
rows instead of a corrupted table.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SUPPORTED_SCHEMA_VERSIONS = (1,)
CORRUPT_RECORD_COLUMN = "_corrupt_record"
MONEY = DecimalType(14, 2)

# --------------------------------------------------------------------------- wire contracts (Kafka JSON)

TRANSACTION_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("transaction_id", StringType()),
        # Parsed explicitly downstream so bad timestamps can be quarantined with a reason.
        StructField("event_ts", StringType()),
        StructField("user_id", StringType()),
        StructField("card_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("amount", MONEY),
        StructField("currency", StringType()),
        StructField("channel", StringType()),
        StructField("entry_mode", StringType()),
        StructField("location_lat", DoubleType()),
        StructField("location_lon", DoubleType()),
        StructField("city", StringType()),
        StructField("country_code", StringType()),
    ]
)

CHARGEBACK_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("chargeback_id", StringType()),
        StructField("transaction_id", StringType()),
        StructField("user_id", StringType()),
        StructField("reason_code", StringType()),
        StructField("amount", MONEY),
        StructField("currency", StringType()),
        StructField("reported_ts", StringType()),
    ]
)


def with_corrupt_record(schema: StructType) -> StructType:
    """Add the column that ``from_json`` fills with the raw text of unparseable records."""
    return StructType([*schema.fields, StructField(CORRUPT_RECORD_COLUMN, StringType())])


# --------------------------------------------------------------------------- bronze

BRONZE_SCHEMA = StructType(
    [
        StructField("message_key", StringType()),
        StructField("raw_payload", StringType()),
        StructField("topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_ts", TimestampType()),
        StructField("ingest_ts", TimestampType()),
        StructField("ingest_date", DateType()),
        StructField("ingest_hour", IntegerType()),
    ]
)

# --------------------------------------------------------------------------- silver

SILVER_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("user_id", StringType(), False),
        StructField("card_id", StringType()),
        StructField("merchant_id", StringType(), False),
        StructField("amount", MONEY, False),
        StructField("currency", StringType(), False),
        StructField("amount_usd", MONEY, False),
        StructField("channel", StringType()),
        StructField("entry_mode", StringType()),
        StructField("location_lat", DoubleType()),
        StructField("location_lon", DoubleType()),
        StructField("city", StringType()),
        StructField("country_code", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_ts", TimestampType()),
    ]
)

SILVER_CHARGEBACK_SCHEMA = StructType(
    [
        StructField("chargeback_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("reason_code", StringType(), False),
        StructField("amount", MONEY, False),
        StructField("currency", StringType(), False),
        StructField("amount_usd", MONEY, False),
        StructField("reported_ts", TimestampType(), False),
        StructField("kafka_ts", TimestampType()),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("raw_payload", StringType()),
        StructField("dq_errors", ArrayType(StringType())),
        StructField("topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_ts", TimestampType()),
        StructField("quarantined_at", TimestampType()),
        StructField("quarantine_date", DateType()),
    ]
)

# --------------------------------------------------------------------------- gold: stateful feature operator

# Per-card state carried between micro-batches by applyInPandasWithState.
USER_FEATURE_STATE_SCHEMA = StructType(
    [
        StructField("recent_ts", ArrayType(LongType())),
        StructField("recent_amount", ArrayType(DoubleType())),
        StructField("recent_merchant", ArrayType(StringType())),
        StructField("recent_lat", ArrayType(DoubleType())),
        StructField("recent_lon", ArrayType(DoubleType())),
        StructField("last_ts", LongType()),
        StructField("last_lat", DoubleType()),
        StructField("last_lon", DoubleType()),
        StructField("n_obs", LongType()),
        StructField("mean_log_amount", DoubleType()),
        StructField("m2_log_amount", DoubleType()),
        StructField("ewma_gap_s", DoubleType()),
    ]
)

# Features appended to each silver row by the stateful operator.
USER_FEATURE_FIELDS = [
    StructField("txn_count_1m", IntegerType()),
    StructField("txn_count_5m", IntegerType()),
    StructField("amount_usd_5m", DoubleType()),
    StructField("distinct_merchants_5m", IntegerType()),
    StructField("small_txn_count_5m", IntegerType()),
    StructField("seconds_since_prev", DoubleType()),
    StructField("km_from_prev", DoubleType()),
    StructField("implied_speed_kmh", DoubleType()),
    StructField("amount_zscore", DoubleType()),
    StructField("baseline_txn_per_min", DoubleType()),
    StructField("history_count", LongType()),
]

USER_FEATURE_OUTPUT_SCHEMA = StructType(
    [StructField(f.name, f.dataType, True) for f in SILVER_TRANSACTION_SCHEMA.fields] + USER_FEATURE_FIELDS
)

# --------------------------------------------------------------------------- gold

# Columns added by dimension enrichment and the rule engine.
GOLD_ENRICHMENT_FIELDS = [
    StructField("user_key", LongType()),
    StructField("merchant_key", LongType()),
    StructField("location_key", IntegerType()),
    StructField("date_key", IntegerType()),
    StructField("mcc", StringType()),
    StructField("category_risk_tier", StringType()),
    StructField("has_chip_card", BooleanType()),
    StructField("account_age_days", IntegerType()),
    StructField("distance_from_home_km", DoubleType()),
    StructField("fraud_score", DoubleType()),
    StructField("reason_codes", ArrayType(StringType())),
    StructField("risk_level", StringType()),
    StructField("is_flagged", BooleanType()),
    StructField("rules_version", StringType()),
    StructField("processed_at", TimestampType()),
]

GOLD_TRANSACTION_SCHEMA = StructType([*USER_FEATURE_OUTPUT_SCHEMA.fields, *GOLD_ENRICHMENT_FIELDS])

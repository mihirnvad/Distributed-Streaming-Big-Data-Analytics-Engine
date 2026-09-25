"""Bronze layer: Kafka -> Delta Lake, raw and replayable.

Bronze stores every Kafka record exactly as received (payload kept as a string,
no parsing), plus its Kafka coordinates. Parsing bugs downstream can therefore
always be fixed by replaying bronze - Kafka retention no longer bounds recovery.

Fault tolerance: Spark tracks the consumed Kafka offsets for every micro-batch in
the checkpoint's write-ahead offset log *before* processing it, and the Delta sink
records the batch id in its transaction log. Replays after a crash are detected
and skipped, giving exactly-once delivery from Kafka into the bronze table.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common.settings import Settings
from streaming.schemas import BRONZE_SCHEMA
from streaming.spark_session import ensure_delta_table, scheduler_pool, with_trigger

BRONZE_PARTITIONS = ["ingest_date", "ingest_hour"]


def read_kafka(spark: SparkSession, settings: Settings, topic: str) -> DataFrame:
    s = settings.streaming
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", s.starting_offsets)
        # Backpressure: cap each micro-batch so a backlog drains in bounded batches
        # instead of one enormous batch that blows the latency SLO.
        .option("maxOffsetsPerTrigger", s.max_offsets_per_trigger)
        # Fail loudly if Kafka deleted data we never read (retention shorter than an outage).
        .option("failOnDataLoss", "true")
        .load()
    )


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Project Kafka records onto the bronze contract. Partitioning uses the Kafka
    timestamp (not wall-clock) so replays land in the same partitions."""
    return kafka_df.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_payload"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
        F.to_date("timestamp").alias("ingest_date"),
        F.hour("timestamp").alias("ingest_hour"),
    )


def _start_topic(spark: SparkSession, settings: Settings, topic: str, path: str, name: str, trigger: str):
    ensure_delta_table(spark, path, BRONZE_SCHEMA, BRONZE_PARTITIONS)
    with scheduler_pool(spark, "bronze"):
        writer = (
            to_bronze(read_kafka(spark, settings, topic))
            .writeStream.queryName(name)
            .format("delta")
            .outputMode("append")
            .partitionBy(*BRONZE_PARTITIONS)
            .option("checkpointLocation", settings.lakehouse.checkpoint(name))
        )
        return with_trigger(writer, trigger).start(path)


def start(spark: SparkSession, settings: Settings) -> list[StreamingQuery]:
    lake, kafka, s = settings.lakehouse, settings.kafka, settings.streaming
    return [
        _start_topic(
            spark, settings, kafka.transactions_topic, lake.bronze_transactions, "bronze_transactions",
            s.trigger_interval,
        ),
        _start_topic(
            spark, settings, kafka.chargebacks_topic, lake.bronze_chargebacks, "bronze_chargebacks",
            s.slow_trigger_interval,
        ),
    ]  # fmt: skip

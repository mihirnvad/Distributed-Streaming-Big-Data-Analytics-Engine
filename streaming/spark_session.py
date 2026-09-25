"""SparkSession construction and small runtime helpers shared by the jobs."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import StructField, StructType

log = logging.getLogger(__name__)


def build_spark(app_name: str, extra_conf: dict[str, str] | None = None) -> SparkSession:
    """Create (or reuse) the SparkSession.

    Cluster-wide settings (Delta extensions, RocksDB state store, packages...) live
    in ``config/spark-defaults.conf`` so the same code runs locally and on a cluster.
    """
    builder = SparkSession.builder.appName(app_name)
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def _nullable(schema: StructType) -> StructType:
    return StructType([StructField(f.name, f.dataType, True, f.metadata) for f in schema.fields])


def ensure_delta_table(spark: SparkSession, path: str, schema: StructType, partition_by: list[str]) -> None:
    """Create an empty Delta table with the declared schema if none exists yet.

    Downstream streams read their upstream table from the very first trigger, so the
    tables must exist (with their contract schema) before any query starts.
    """
    (
        spark.createDataFrame([], _nullable(schema))
        .write.format("delta")
        .mode("ignore")
        .partitionBy(*partition_by)
        .save(path)
    )


@contextmanager
def scheduler_pool(spark: SparkSession, pool: str) -> Iterator[None]:
    """Run queries started inside this block in their own FAIR scheduler pool.

    Without pools, a heavy micro-batch in one query (e.g. the stateful gold job)
    would starve the cheap bronze ingestion query that shares the executors.
    """
    sc = spark.sparkContext
    previous = sc.getLocalProperty("spark.scheduler.pool")
    sc.setLocalProperty("spark.scheduler.pool", pool)
    try:
        yield
    finally:
        sc.setLocalProperty("spark.scheduler.pool", previous)  # type: ignore[arg-type]


def with_trigger(writer: DataStreamWriter, interval: str) -> DataStreamWriter:
    """``available-now`` drains the backlog then stops (backfills/tests); otherwise a processing-time trigger."""
    if interval.lower() in {"available-now", "availablenow"}:
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=interval)

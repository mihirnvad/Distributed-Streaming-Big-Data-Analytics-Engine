"""Idempotent, parallel bulk upserts from Spark into PostgreSQL.

Spark's built-in JDBC sink can only INSERT, so a replayed micro-batch (after a
crash between the sink write and the checkpoint commit) would duplicate rows.
Instead every executor partition:

1. opens one connection and a transaction,
2. ``COPY``s its rows into a session-private temp table (fastest bulk path),
3. merges them with ``INSERT ... ON CONFLICT`` (DO NOTHING or DO UPDATE),
4. runs optional follow-up statements against the same staged rows,
5. commits.

Keyed on natural/business keys, replaying the same batch is a no-op, which turns
Spark's at-least-once foreachBatch into effectively-exactly-once delivery.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from pyspark.sql import DataFrame

log = logging.getLogger(__name__)

STAGING_TABLE = "_upsert_stage"


@dataclass(frozen=True)
class UpsertSpec:
    table: str
    columns: tuple[str, ...]
    conflict_columns: tuple[str, ...]
    update_columns: tuple[str, ...] = ()  # empty -> ON CONFLICT DO NOTHING
    touch_column: str | None = None  # set to now() on update, e.g. "updated_at"
    post_statements: tuple[str, ...] = ()  # may reference the staged rows as {staging}

    def merge_sql(self) -> str:
        cols = ", ".join(self.columns)
        conflict = ", ".join(self.conflict_columns)
        if self.update_columns:
            assignments = [f"{c} = EXCLUDED.{c}" for c in self.update_columns]
            if self.touch_column:
                assignments.append(f"{self.touch_column} = now()")
            action = "DO UPDATE SET " + ", ".join(assignments)
        else:
            action = "DO NOTHING"
        return f"INSERT INTO {self.table} ({cols}) SELECT {cols} FROM {STAGING_TABLE} ON CONFLICT ({conflict}) {action}"


def write_partition(rows: Iterable, spec: UpsertSpec, dsn: str) -> int:
    """Upsert one partition's rows; returns the number of staged rows."""
    import psycopg  # imported on the executor

    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return 0

    staged = 0
    with psycopg.connect(dsn, autocommit=False, application_name="spark-upsert") as conn:
        # The lakehouse is the system of record and replays are idempotent, so trading
        # a few ms of durability window for ingest throughput is safe here.
        conn.execute("SET LOCAL synchronous_commit = off")
        conn.execute(
            f"CREATE TEMP TABLE {STAGING_TABLE} ON COMMIT DROP AS "
            f"SELECT {', '.join(spec.columns)} FROM {spec.table} WITH NO DATA"
        )
        with conn.cursor() as cur:
            with cur.copy(f"COPY {STAGING_TABLE} ({', '.join(spec.columns)}) FROM STDIN") as copy:
                copy.write_row(tuple(first))
                staged = 1
                for row in iterator:
                    copy.write_row(tuple(row))
                    staged += 1
            cur.execute(spec.merge_sql())
            for statement in spec.post_statements:
                cur.execute(statement.format(staging=STAGING_TABLE))
        conn.commit()
    return staged


def upsert_dataframe(df: DataFrame, spec: UpsertSpec, dsn: str, max_partitions: int = 4) -> None:
    """Upsert a (small, micro-batch sized) DataFrame into Postgres from the executors."""
    projected = df.select(*spec.columns)
    if projected.rdd.getNumPartitions() > max_partitions:
        projected = projected.coalesce(max_partitions)
    projected.foreachPartition(lambda rows: write_partition(rows, spec, dsn))

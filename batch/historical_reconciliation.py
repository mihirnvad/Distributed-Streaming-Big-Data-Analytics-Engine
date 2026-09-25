"""Nightly batch reconciliation between the lakehouse and the serving warehouse.

The streaming job keeps Postgres current within seconds, but a serving database
is not a system of record: it can lose rows (restore from backup, manual
cleanup, an outage longer than retry budgets). This job treats the gold Delta
table as the source of truth for one business date and:

1. ensures upcoming warehouse partitions exist,
2. diffs lakehouse vs. warehouse transaction ids and backfills anything missing,
3. rebuilds ``dw.agg_hourly_merchant_risk`` from the lakehouse (+ chargebacks),
4. resolves alert case status: fraud chargeback -> CONFIRMED_FRAUD; no dispute
   after the label-maturity window -> FALSE_POSITIVE,
5. summarises quarantined records by data-quality reason,
6. compacts (OPTIMIZE) the day's Delta partitions and VACUUMs old files,
7. records an auditable run row in ``ops.reconciliation_runs``.

Every step is idempotent, so the job can be re-run for any date.

    spark-submit batch/historical_reconciliation.py --date 2026-09-24
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.settings import Settings, load_settings
from streaming.gold_sink import FACT_FRAUD_ALERTS, FACT_TRANSACTIONS
from streaming.postgres_writer import UpsertSpec, upsert_dataframe
from streaming.spark_session import build_spark

log = logging.getLogger("reconciliation")

AGG_HOURLY_MERCHANT = UpsertSpec(
    table="dw.agg_hourly_merchant_risk",
    columns=(
        "hour_ts", "date_key", "merchant_key", "mcc", "txn_count", "total_amount_usd", "flagged_count",
        "avg_fraud_score", "chargeback_count", "fraud_chargeback_count", "chargeback_amount_usd",
    ),
    conflict_columns=("hour_ts", "merchant_key"),
    update_columns=(
        "date_key", "mcc", "txn_count", "total_amount_usd", "flagged_count", "avg_fraud_score",
        "chargeback_count", "fraud_chargeback_count", "chargeback_amount_usd",
    ),
    touch_column="computed_at",
)  # fmt: skip

FRAUD_REASON_CODES = ("10.1", "10.3", "10.4")


def _day_bounds(day: date) -> tuple[str, str]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start.isoformat(), (start + timedelta(days=1)).isoformat()


def warehouse_transaction_ids(spark: SparkSession, settings: Settings, day: date) -> DataFrame:
    """Read the day's transaction ids from Postgres in 24 parallel hourly slices."""
    start, _ = _day_bounds(day)
    predicates = [
        f"event_ts >= timestamptz '{start}' + interval '{h} hours' "
        f"AND event_ts < timestamptz '{start}' + interval '{h + 1} hours'"
        for h in range(24)
    ]
    pg = settings.postgres
    return spark.read.jdbc(
        url=pg.jdbc_url,
        table="(SELECT transaction_id::text AS transaction_id, event_ts FROM dw.fact_transactions) t",
        predicates=predicates,
        properties=pg.jdbc_properties,
    ).select("transaction_id")


def hourly_merchant_risk(gold_day: DataFrame, chargebacks: DataFrame) -> DataFrame:
    cb = chargebacks.select(
        "transaction_id",
        F.lit(1).alias("has_chargeback"),
        F.col("reason_code").isin(*FRAUD_REASON_CODES).cast("int").alias("is_fraud_chargeback"),
        F.col("amount_usd").alias("chargeback_amount_usd"),
    ).dropDuplicates(["transaction_id"])
    return (
        gold_day.join(cb, "transaction_id", "left")
        .groupBy(F.date_trunc("hour", "event_ts").alias("hour_ts"), "date_key", "merchant_key", "mcc")
        .agg(
            F.count(F.lit(1)).cast("int").alias("txn_count"),
            F.sum("amount_usd").cast("decimal(16,2)").alias("total_amount_usd"),
            F.sum(F.col("is_flagged").cast("int")).cast("int").alias("flagged_count"),
            F.round(F.avg("fraud_score"), 4).cast("decimal(6,4)").alias("avg_fraud_score"),
            F.coalesce(F.sum("has_chargeback"), F.lit(0)).cast("int").alias("chargeback_count"),
            F.coalesce(F.sum("is_fraud_chargeback"), F.lit(0)).cast("int").alias("fraud_chargeback_count"),
            F.coalesce(F.sum("chargeback_amount_usd"), F.lit(0)).cast("decimal(16,2)").alias("chargeback_amount_usd"),
        )
    )


def resolve_alert_status(settings: Settings, label_maturity_hours: int) -> tuple[int, int]:
    import psycopg

    with psycopg.connect(settings.postgres.dsn) as conn:
        confirmed = conn.execute(
            """
            UPDATE dw.fact_fraud_alerts a
               SET status = 'CONFIRMED_FRAUD', status_updated_at = now()
             WHERE a.status <> 'CONFIRMED_FRAUD'
               AND EXISTS (SELECT 1 FROM dw.fact_chargebacks c
                             JOIN dw.dim_dispute_reason r ON r.reason_code = c.reason_code AND r.is_fraud
                            WHERE c.transaction_id = a.transaction_id)
            """
        ).rowcount
        cleared = conn.execute(
            """
            UPDATE dw.fact_fraud_alerts a
               SET status = 'FALSE_POSITIVE', status_updated_at = now()
             WHERE a.status = 'OPEN'
               AND a.event_ts < now() - make_interval(hours => %s)
               AND NOT EXISTS (SELECT 1 FROM dw.fact_chargebacks c WHERE c.transaction_id = a.transaction_id)
            """,
            (label_maturity_hours,),
        ).rowcount
    return confirmed, cleared


def maintain_delta_tables(spark: SparkSession, settings: Settings, day: date, retain_hours: int) -> None:
    lake = settings.lakehouse
    targets = [
        (lake.bronze_transactions, f"ingest_date = '{day}'"),
        (lake.bronze_chargebacks, f"ingest_date = '{day}'"),
        (lake.silver_transactions, f"event_date = '{day}'"),
        (lake.gold_transactions, f"event_date = '{day}'"),
        (lake.silver_chargebacks, None),
        (lake.silver_quarantine, f"quarantine_date = '{day}'"),
    ]
    for path, predicate in targets:
        where = f" WHERE {predicate}" if predicate else ""
        started = time.perf_counter()
        metrics = spark.sql(f"OPTIMIZE delta.`{path}`{where}").select("metrics.*").first()
        log.info(
            "OPTIMIZE %s%s: removed=%s added=%s (%.1fs)",
            path,
            where,
            metrics["numFilesRemoved"] if metrics else "?",
            metrics["numFilesAdded"] if metrics else "?",
            time.perf_counter() - started,
        )
        spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")


def run(spark: SparkSession, settings: Settings, day: date, args: argparse.Namespace) -> dict:
    import psycopg

    lake = settings.lakehouse
    started_at = datetime.now(timezone.utc)
    with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
        run_id = conn.execute(
            "INSERT INTO ops.reconciliation_runs (business_date, started_at, status) "
            "VALUES (%s, %s, 'RUNNING') RETURNING run_id",
            (day, started_at),
        ).fetchone()[0]
        conn.execute("SELECT dw.ensure_daily_partitions(%s, 30)", (day - timedelta(days=1),))

    try:
        gold_day = spark.read.format("delta").load(lake.gold_transactions).where(F.col("event_date") == F.lit(day))
        gold_day = gold_day.dropDuplicates(["transaction_id"]).cache()
        lakehouse_rows = gold_day.count()

        warehouse_ids = warehouse_transaction_ids(spark, settings, day).cache()
        warehouse_rows = warehouse_ids.count()

        missing = gold_day.join(warehouse_ids, "transaction_id", "left_anti").cache()
        backfilled = missing.count()
        if backfilled:
            log.warning("backfilling %d transactions missing from the warehouse", backfilled)
            facts = missing.select(
                "*",
                F.col("currency").alias("currency_code"),
                F.col("location_lat").alias("txn_lat"),
                F.col("location_lon").alias("txn_lon"),
            )
            upsert_dataframe(facts, FACT_TRANSACTIONS, settings.postgres.dsn, max_partitions=8)
            upsert_dataframe(missing.where("is_flagged"), FACT_FRAUD_ALERTS, settings.postgres.dsn, max_partitions=8)
        orphans = warehouse_ids.join(gold_day.select("transaction_id"), "transaction_id", "left_anti").count()
        if orphans:
            log.error("%d warehouse rows have no lakehouse record for %s", orphans, day)

        chargebacks = spark.read.format("delta").load(lake.silver_chargebacks)
        hourly = hourly_merchant_risk(gold_day, chargebacks)
        upsert_dataframe(hourly, AGG_HOURLY_MERCHANT, settings.postgres.dsn, max_partitions=4)

        confirmed, cleared = resolve_alert_status(settings, args.label_maturity_hours)

        quarantine = (
            spark.read.format("delta")
            .load(lake.silver_quarantine)
            .where(F.col("quarantine_date") == F.lit(day))
            .select(F.explode("dq_errors").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )
        dq_summary = {row["reason"]: row["count"] for row in quarantine}
        quarantined_rows = (
            spark.read.format("delta")
            .load(lake.silver_quarantine)
            .where(F.col("quarantine_date") == F.lit(day))
            .count()
        )

        if not args.skip_maintenance:
            maintain_delta_tables(spark, settings, day, args.vacuum_retain_hours)

        summary = {
            "lakehouse_rows": lakehouse_rows,
            "warehouse_rows": warehouse_rows,
            "backfilled_rows": backfilled,
            "orphan_rows": orphans,
            "alerts_confirmed": confirmed,
            "alerts_cleared": cleared,
            "quarantined_rows": quarantined_rows,
        }
        details = {"quarantine_reasons": dq_summary, "maintenance": not args.skip_maintenance}
        with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
            conn.execute(
                """
                UPDATE ops.reconciliation_runs
                   SET status = 'SUCCEEDED', finished_at = now(), lakehouse_rows = %(lakehouse_rows)s,
                       warehouse_rows = %(warehouse_rows)s, backfilled_rows = %(backfilled_rows)s,
                       orphan_rows = %(orphan_rows)s, alerts_confirmed = %(alerts_confirmed)s,
                       alerts_cleared = %(alerts_cleared)s, quarantined_rows = %(quarantined_rows)s,
                       details = %(details)s
                 WHERE run_id = %(run_id)s
                """,
                {**summary, "details": json.dumps(details), "run_id": run_id},
            )
        return {**summary, **details}
    except Exception as exc:
        with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
            conn.execute(
                "UPDATE ops.reconciliation_runs SET status = 'FAILED', finished_at = now(), details = %s "
                "WHERE run_id = %s",
                (json.dumps({"error": repr(exc)}), run_id),
            )
        raise


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", type=date.fromisoformat, default=None, help="business date (UTC); default today")
    parser.add_argument(
        "--label-maturity-hours",
        type=int,
        default=24,
        help="alerts with no dispute after this many hours are marked FALSE_POSITIVE (real world: ~90 days)",
    )
    parser.add_argument("--vacuum-retain-hours", type=int, default=168)
    parser.add_argument("--skip-maintenance", action="store_true", help="skip OPTIMIZE/VACUUM")
    args = parser.parse_args(argv)

    day = args.date or datetime.now(timezone.utc).date()
    settings = load_settings()
    spark = build_spark(f"fraud-reconciliation-{day}")
    started = time.perf_counter()
    summary = run(spark, settings, day, args)
    log.info("reconciliation for %s finished in %.1fs: %s", day, time.perf_counter() - started, json.dumps(summary))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Warehouse integration tests against a real PostgreSQL.

Creates a throwaway database, applies ``sql/init_schema.sql``, seeds dimensions
with the real seeder, loads synthetic facts, then executes every analytical model
in ``sql/analytics_kpis.sql`` and every dashboard query.

    TEST_POSTGRES_DSN=postgresql://fraud:fraud@localhost:5432/postgres pytest -m integration
"""

from __future__ import annotations

import os
import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

if not os.environ.get("TEST_POSTGRES_DSN"):
    pytest.skip("TEST_POSTGRES_DSN not set", allow_module_level=True)

psycopg = pytest.importorskip("psycopg")
from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402

from dashboard.queries import ALL_QUERIES, TIME_WINDOWS  # noqa: E402
from producer.entities import build_population  # noqa: E402
from producer.seed_dimensions import seed_merchants, seed_reference, seed_users  # noqa: E402

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
RULES = ["VELOCITY_BURST", "CARD_TESTING", "AMOUNT_SPIKE", "IMPOSSIBLE_TRAVEL", "HIGH_RISK_MCC_LARGE_TICKET"]


def analytics_models() -> dict[str, str]:
    text = (ROOT / "sql" / "analytics_kpis.sql").read_text(encoding="utf-8")
    parts = re.split(r"^-- name: (\w+)\s*$", text, flags=re.M)
    return {parts[i]: parts[i + 1].strip() for i in range(1, len(parts), 2)}


def _load_facts(conn, rng: random.Random) -> None:
    users = [r[0] for r in conn.execute("SELECT user_key FROM dw.dim_users WHERE user_key > 0")]
    merchants = [r[0] for r in conn.execute("SELECT merchant_key FROM dw.dim_merchants WHERE merchant_key > 0")]
    locations = conn.execute(
        "SELECT location_key, latitude, longitude FROM dw.dim_location WHERE location_key > 0"
    ).fetchall()
    now = datetime.now(timezone.utc)
    facts, alerts, chargebacks = [], [], []
    for i in range(4_000):
        # 80% in the last 3 hours, the rest 24-30 hours ago (previous-period comparisons)
        age = timedelta(minutes=rng.uniform(6, 180)) if i % 5 else timedelta(hours=rng.uniform(24, 30))
        ts = now - age
        loc_key, lat, lon = rng.choice(locations)
        score = round(min(1.0, rng.betavariate(0.6, 6) + (0.7 if i % 37 == 0 else 0)), 4)
        flagged = score >= 0.6
        reasons = rng.sample(RULES, k=rng.randint(1, 2)) if score >= 0.3 else []
        user_key = rng.choice(users[:60])  # concentrate activity so per-card sequences exist
        amount = round(rng.lognormvariate(3.6, 0.8), 2)
        txn_id = str(uuid.uuid4())
        facts.append(
            (txn_id, ts, int(ts.strftime("%Y%m%d")), user_key, rng.choice(merchants), loc_key, amount, "USD", amount,
             "POS", "CHIP", lat + rng.gauss(0, 0.01), lon + rng.gauss(0, 0.01), 5.0, 1.0, 60.0, 60.0, 1, 2, amount,
             2, 0, 0.3, score, "HIGH" if flagged else "LOW", flagged, reasons, 0, i, ts + timedelta(seconds=1), ts)
        )  # fmt: skip
        if flagged:
            alerts.append(
                (txn_id, ts, int(ts.strftime("%Y%m%d")), user_key, facts[-1][4], amount, score, "HIGH", reasons)
            )
        if (flagged and rng.random() < 0.7) or rng.random() < 0.004:
            code = "10.4" if flagged or rng.random() < 0.5 else "13.1"
            chargebacks.append(
                (str(uuid.uuid4()), txn_id, user_key, code, amount, "USD", amount, ts + timedelta(minutes=2))
            )

    with conn.cursor() as cur:
        with cur.copy(
            "COPY dw.fact_transactions (transaction_id, event_ts, date_key, user_key, merchant_key, location_key, "
            "amount, currency_code, amount_usd, channel, entry_mode, txn_lat, txn_lon, distance_from_home_km, "
            "km_from_prev, seconds_since_prev, implied_speed_kmh, txn_count_1m, txn_count_5m, amount_usd_5m, "
            "distinct_merchants_5m, small_txn_count_5m, amount_zscore, fraud_score, risk_level, is_flagged, "
            "reason_codes, kafka_partition, kafka_offset, kafka_ts, processed_at) FROM STDIN"
        ) as copy:
            for row in facts:
                copy.write_row(row)
        cur.executemany(
            "INSERT INTO dw.fact_fraud_alerts (transaction_id, event_ts, date_key, user_key, merchant_key, amount_usd, "
            "fraud_score, risk_level, reason_codes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            alerts,
        )
        cur.executemany(
            "INSERT INTO dw.fact_chargebacks (chargeback_id, transaction_id, user_key, reason_code, amount, "
            "currency_code, amount_usd, reported_ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            chargebacks,
        )
        for m in merchants[:25]:
            for k in range(12):
                start = now.replace(second=0, microsecond=0) - timedelta(minutes=k + 5)
                cur.execute(
                    "INSERT INTO dw.agg_merchant_window_risk (merchant_key, window_start, window_end, txn_count, "
                    "total_amount_usd, max_amount_usd, approx_distinct_users, flagged_count, avg_fraud_score) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (m, start, start + timedelta(minutes=5), rng.randint(1, 40), 500, 90, 5, rng.randint(0, 3), 0.1),
                )
        for q, n in (("bronze_transactions", 30), ("gold_transactions", 30), ("silver_transactions", 30)):
            run_id = uuid.uuid4()
            for b in range(n):
                cur.execute(
                    "INSERT INTO ops.streaming_query_progress (run_id, batch_id, query_id, query_name, progress_ts, "
                    "num_input_rows, input_rows_per_second, processed_rows_per_second, batch_duration_ms, "
                    "max_offsets_behind_latest, rows_dropped_by_watermark) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (run_id, b, uuid.uuid4(), q, now - timedelta(seconds=2 * (n - b)), 2000, 1000.0, 2500.0,
                     rng.randint(300, 1900), 0, 0),
                )  # fmt: skip


@pytest.fixture(scope="module")
def warehouse_dsn():
    admin_dsn = os.environ["TEST_POSTGRES_DSN"]
    dbname = f"fraud_test_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{dbname}"')
    params = conninfo_to_dict(admin_dsn)
    params["dbname"] = dbname
    dsn = make_conninfo(**params)
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute((ROOT / "sql" / "init_schema.sql").read_text(encoding="utf-8"))
        population = build_population(n_users=400, n_merchants=700, seed=3)
        with psycopg.connect(dsn) as conn:
            seed_reference(conn)
            seed_merchants(conn, population.merchants)
            seed_users(conn, population.users, upgrades=set())
            _load_facts(conn, random.Random(3))
            conn.commit()
        yield dsn
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')


def test_init_schema_is_idempotent(warehouse_dsn):
    with psycopg.connect(warehouse_dsn, autocommit=True) as conn:
        conn.execute((ROOT / "sql" / "init_schema.sql").read_text(encoding="utf-8"))
        assert conn.execute("SELECT count(*) FROM dw.dim_users WHERE user_key = -1").fetchone()[0] == 1


@pytest.mark.parametrize("name", sorted(analytics_models()))
def test_analytics_model_executes(warehouse_dsn, name):
    sql = analytics_models()[name]
    with psycopg.connect(warehouse_dsn, autocommit=True) as conn:
        rows = conn.execute(sql).fetchall()
    must_return_rows = {
        "executive_kpi_snapshot",
        "riskiest_merchant_categories",
        "rule_effectiveness",
        "alert_threshold_tuning_curve",
        "fraud_incidents_time_to_detect",
        "customer_spend_deciles",
        "geography_rollup",
        "chargeback_latency_by_reason",
        "weekday_hour_risk_heatmap",
        "pipeline_latency_slo",
        "trailing_30d_customer_fraud_rate",
    }
    if name in must_return_rows:
        assert rows, f"{name} returned no rows"


def test_model_results_are_sane(warehouse_dsn):
    models = analytics_models()
    with psycopg.connect(warehouse_dsn, autocommit=True) as conn:
        top5 = conn.execute(models["riskiest_merchant_categories"]).fetchall()
        curve = conn.execute(models["alert_threshold_tuning_curve"]).fetchall()
        rollup = conn.execute(models["geography_rollup"]).fetchall()
    assert len(top5) == 5 and [r[0] for r in top5] == sorted(r[0] for r in top5)
    recalls = [float(r[4]) for r in curve if r[4] is not None]
    assert recalls == sorted(recalls, reverse=True), "recall must fall as the threshold rises"
    assert rollup[0][:2] == ("ALL", "Grand total")


@pytest.mark.parametrize("name", sorted(ALL_QUERIES))
@pytest.mark.parametrize("window", TIME_WINDOWS[:2], ids=lambda w: w.window.replace(" ", ""))
def test_dashboard_query_executes(warehouse_dsn, name, window):
    sql = ALL_QUERIES[name]
    params = {"window": window.window, "bucket": window.bucket} if "%(" in sql else None
    with psycopg.connect(warehouse_dsn, autocommit=True) as conn:
        conn.execute(sql, params).fetchall()


def test_scd2_merge_versions_changed_cardholders(warehouse_dsn):
    population = build_population(n_users=400, n_merchants=700, seed=3)
    upgraded = {u.user_id for u in population.users if u.card_tier != "PLATINUM"}
    upgraded = set(sorted(upgraded)[:5])
    with psycopg.connect(warehouse_dsn) as conn:
        current_before = conn.execute("SELECT count(*) FROM dw.dim_users WHERE is_current").fetchone()[0]
        inserted, expired = seed_users(conn, population.users, upgrades=upgraded)
        conn.commit()
        assert (inserted, expired) == (5, 5)
        assert conn.execute("SELECT count(*) FROM dw.dim_users WHERE is_current").fetchone()[0] == current_before
        versions = conn.execute(
            "SELECT count(*) FROM dw.dim_users WHERE user_id = ANY(%s)", (list(upgraded),)
        ).fetchone()[0]
        assert versions == 10
        # re-running with identical attributes is a no-op
        assert seed_users(conn, population.users, upgrades=upgraded) == (0, 0)
        conn.commit()


def test_partition_maintenance_moves_rows_out_of_default(warehouse_dsn):
    far_future = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0) + timedelta(days=90)
    with psycopg.connect(warehouse_dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO dw.fact_transactions (transaction_id, event_ts, date_key, user_key, merchant_key, "
            "location_key, amount, currency_code, amount_usd, channel, entry_mode, fraud_score, risk_level, "
            "is_flagged) VALUES (gen_random_uuid(), %s, 0, -1, -1, -1, 1, 'USD', 1, 'POS', 'CHIP', 0, 'LOW', false)",
            (far_future,),
        )
        where = "event_ts = %s"
        assert conn.execute(f"SELECT tableoid::regclass::text FROM dw.fact_transactions WHERE {where}",
                            (far_future,)).fetchone()[0] == "dw.fact_transactions_default"  # fmt: skip
        created = conn.execute("SELECT dw.ensure_daily_partitions(%s::date, 1)", (far_future.date(),)).fetchone()[0]
        assert created == 1
        part = conn.execute(f"SELECT tableoid::regclass::text FROM dw.fact_transactions WHERE {where}",
                            (far_future,)).fetchone()[0]  # fmt: skip
        assert part == f"dw.fact_transactions_{far_future:%Y%m%d}"


def test_upsert_writer_is_idempotent_and_confirms_alerts(warehouse_dsn):
    pytest.importorskip("pyspark")
    from streaming.gold_sink import FACT_CHARGEBACKS
    from streaming.postgres_writer import write_partition

    with psycopg.connect(warehouse_dsn, autocommit=True) as conn:
        txn_id, user_key, amount = conn.execute(
            "SELECT a.transaction_id, a.user_key, a.amount_usd FROM dw.fact_fraud_alerts a "
            "WHERE NOT EXISTS (SELECT 1 FROM dw.fact_chargebacks c WHERE c.transaction_id = a.transaction_id) LIMIT 1"
        ).fetchone()
        conn.execute("UPDATE dw.fact_fraud_alerts SET status = 'OPEN' WHERE transaction_id = %s", (txn_id,))
    row = (str(uuid.uuid4()), str(txn_id), user_key, "10.4", amount, "USD", amount, datetime.now(timezone.utc))

    assert write_partition([row], FACT_CHARGEBACKS, warehouse_dsn) == 1
    assert write_partition([row], FACT_CHARGEBACKS, warehouse_dsn) == 1  # replayed micro-batch
    assert write_partition([], FACT_CHARGEBACKS, warehouse_dsn) == 0
    with psycopg.connect(warehouse_dsn, autocommit=True) as conn:
        count = conn.execute("SELECT count(*) FROM dw.fact_chargebacks WHERE chargeback_id = %s", (row[0],)).fetchone()
        status = conn.execute("SELECT status FROM dw.fact_fraud_alerts WHERE transaction_id = %s", (txn_id,)).fetchone()
    assert count[0] == 1
    assert status[0] == "CONFIRMED_FRAUD"

"""Small pure-Python pieces of the streaming job: SQL generation, durations, metrics."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from streaming.gold_sink import AGG_MERCHANT_WINDOWS, FACT_TRANSACTIONS, GOLD_COLUMNS, parse_duration_ms
from streaming.monitoring import progress_to_row
from streaming.postgres_writer import STAGING_TABLE


def test_insert_only_specs_are_replay_safe():
    sql = FACT_TRANSACTIONS.merge_sql()
    assert sql.startswith("INSERT INTO dw.fact_transactions")
    assert f"FROM {STAGING_TABLE}" in sql
    assert sql.endswith("ON CONFLICT (transaction_id, event_ts) DO NOTHING")


def test_upsert_specs_overwrite_aggregates_and_touch_timestamp():
    sql = AGG_MERCHANT_WINDOWS.merge_sql()
    assert "ON CONFLICT (merchant_key, window_start) DO UPDATE SET" in sql
    assert "txn_count = EXCLUDED.txn_count" in sql
    assert sql.endswith("updated_at = now()")


def test_fact_columns_come_from_the_gold_contract():
    renamed = {"currency_code": "currency", "txn_lat": "location_lat", "txn_lon": "location_lon"}
    for column in FACT_TRANSACTIONS.columns:
        assert renamed.get(column, column) in GOLD_COLUMNS, column


@pytest.mark.parametrize(
    ("text", "ms"),
    [("10 minutes", 600_000), ("1 hour", 3_600_000), ("2 seconds", 2_000), ("500 ms", 500), ("3h", 10_800_000)],
)
def test_parse_duration(text, ms):
    assert parse_duration_ms(text) == ms


def test_parse_duration_rejects_garbage():
    with pytest.raises(ValueError):
        parse_duration_ms("soon")


def test_progress_to_row_extracts_latency_state_and_lag():
    progress = {
        "id": "11111111-1111-1111-1111-111111111111",
        "runId": "22222222-2222-2222-2222-222222222222",
        "name": "gold_transactions",
        "timestamp": "2026-09-24T12:00:00.000Z",
        "batchId": 42,
        "batchDuration": 850,
        "numInputRows": 2000,
        "inputRowsPerSecond": 1000.0,
        "processedRowsPerSecond": 2352.9,
        "durationMs": {"triggerExecution": 850, "addBatch": 700, "getBatch": 5, "queryPlanning": 20, "walCommit": 30},
        "eventTime": {"watermark": "2026-09-24T11:50:00.000Z"},
        "stateOperators": [
            {"numRowsTotal": 5000, "memoryUsedBytes": 1024, "numRowsDroppedByWatermark": 3},
            {"numRowsTotal": 10, "memoryUsedBytes": 24, "numRowsDroppedByWatermark": 0},
        ],
        "sources": [{"metrics": {"maxOffsetsBehindLatest": "17", "avgOffsetsBehindLatest": "4.0"}}],
    }
    row = progress_to_row(progress)
    assert row[:4] == (progress["runId"], 42, progress["id"], "gold_transactions")
    assert row[8] == 850 and row[9] == 850 and row[10] == 700
    assert row[14] == "2026-09-24T11:50:00.000Z"
    assert row[15:19] == (5010, 1048, 3, 17)


def test_progress_to_row_hides_epoch_watermark_and_nan():
    progress = {
        "id": "a",
        "runId": "b",
        "timestamp": "2026-09-24T12:00:00.000Z",
        "batchId": 0,
        "numInputRows": 0,
        "inputRowsPerSecond": float("nan"),
        "eventTime": {"watermark": "1970-01-01T00:00:00.000Z"},
    }
    row = progress_to_row(progress)
    assert row[3] == "a"  # falls back to the query id when unnamed
    assert row[6] is None and row[14] is None
    assert row[15] is None and row[18] is None

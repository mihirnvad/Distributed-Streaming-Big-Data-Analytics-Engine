"""Structured Streaming behaviour tests: watermarks, deduplication, windows, state.

These run real micro-batches through a file source and a memory sink, so they
exercise the same streaming operators (and their state stores) as production.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from common.geo import haversine_km
from streaming.schemas import SILVER_TRANSACTION_SCHEMA

pytestmark = pytest.mark.spark

EVENT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("user_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("amount_usd", DoubleType()),
    ]
)


class JsonDropbox:
    """Atomically drops JSON-lines files into a directory watched by a file stream."""

    def __init__(self, root: Path) -> None:
        self.dir = root / "incoming"
        self.staging = root / "staging"
        self.dir.mkdir()
        self.staging.mkdir()

    def put(self, records: list[dict]) -> None:
        name = f"{uuid.uuid4().hex}.json"
        tmp = self.staging / name
        tmp.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
        os.replace(tmp, self.dir / name)


def _ev(txn_id: str, ts: str, user: str = "U1", amount: float = 10.0) -> dict:
    return {"transaction_id": txn_id, "user_id": user, "event_ts": f"2026-09-24T{ts}Z", "amount_usd": amount}


def test_dedup_within_watermark_spans_micro_batches(spark, tmp_path):
    from streaming.silver_transforms import deduplicate

    box = JsonDropbox(tmp_path)
    stream = spark.readStream.schema(EVENT_SCHEMA).json(str(box.dir))
    query = (
        deduplicate(stream, "transaction_id", "event_ts", "10 minutes")
        .writeStream.format("memory")
        .queryName("dedup_test")
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "cp"))
        .start()
    )
    try:
        box.put([_ev("a", "10:00:00"), _ev("b", "10:01:00"), _ev("a", "10:00:00")])  # duplicate in batch
        query.processAllAvailable()
        box.put([_ev("a", "10:00:00"), _ev("c", "10:02:00")])  # duplicate across batches
        query.processAllAvailable()
        ids = sorted(r.transaction_id for r in spark.table("dedup_test").collect())
        assert ids == ["a", "b", "c"]
    finally:
        query.stop()


def test_sliding_windows_finalise_on_watermark_and_drop_late_data(spark, tmp_path):
    from streaming.silver_transforms import sliding_window_aggregate

    box = JsonDropbox(tmp_path)
    stream = spark.readStream.schema(EVENT_SCHEMA).json(str(box.dir))
    windows = sliding_window_aggregate(
        stream,
        keys=["user_id"],
        event_time="event_ts",
        window_duration="5 minutes",
        slide_duration="1 minute",
        watermark="2 minutes",
        aggregations={"txn_count": F.count(F.lit(1)), "amount": F.sum("amount_usd")},
    )
    query = (
        windows.writeStream.format("memory")
        .queryName("window_test")
        .outputMode("append")  # windows are emitted once, when final
        .option("checkpointLocation", str(tmp_path / "cp"))
        .start()
    )
    try:
        box.put([_ev("1", "10:00:30"), _ev("2", "10:01:30"), _ev("3", "10:02:30")])
        query.processAllAvailable()
        assert spark.table("window_test").count() == 0  # nothing is final yet

        box.put([_ev("4", "10:20:00")])  # advances the watermark to 10:18:00
        query.processAllAvailable()
        box.put([_ev("late", "10:01:00", amount=999.0), _ev("5", "10:20:30")])  # 'late' is behind the watermark
        query.processAllAvailable()

        result = {
            (r.window_start.strftime("%H:%M"), r.window_end.strftime("%H:%M")): (r.txn_count, r.amount)
            for r in spark.table("window_test").collect()
        }
        assert result[("10:00", "10:05")] == (3, 30.0)  # late event did not reopen the window
        assert result[("10:01", "10:06")] == (2, 20.0)
        assert result[("09:56", "10:01")] == (1, 10.0)
        assert ("10:16", "10:21") not in result  # still open (ends after the watermark)

        dropped = sum(
            op.get("numRowsDroppedByWatermark", 0) for p in query.recentProgress for op in p.get("stateOperators", [])
        )
        assert dropped >= 1
    finally:
        query.stop()


def _silver_row(txn_id: str, ts: str, lat: float, lon: float, merchant: str = "M1") -> dict:
    return {
        "transaction_id": txn_id,
        "event_ts": f"2026-09-24T{ts}Z",
        "event_date": "2026-09-24",
        "user_id": "U1",
        "card_id": "tok",
        "merchant_id": merchant,
        "amount": 25.0,
        "currency": "USD",
        "amount_usd": 25.0,
        "channel": "POS",
        "entry_mode": "CHIP",
        "location_lat": lat,
        "location_lon": lon,
        "city": "X",
        "country_code": "US",
        "kafka_partition": 0,
        "kafka_offset": 1,
        "kafka_ts": f"2026-09-24T{ts}Z",
    }


def test_stateful_user_features_carry_state_across_batches(spark, tmp_path):
    from streaming.gold_sink import user_feature_stream

    box = JsonDropbox(tmp_path)
    silver = spark.readStream.schema(SILVER_TRANSACTION_SCHEMA).json(str(box.dir))
    query = (
        user_feature_stream(silver, watermark="10 minutes", idle_timeout="1 hour")
        .writeStream.format("memory")
        .queryName("features_test")
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "cp"))
        .start()
    )
    nyc, london = (40.7128, -74.0060), (51.5074, -0.1278)
    try:
        box.put([_silver_row("t1", "10:00:00", *nyc), _silver_row("t2", "10:00:30", *nyc, merchant="M2")])
        query.processAllAvailable()
        box.put([_silver_row("t3", "10:05:10", *london, merchant="M3")])
        query.processAllAvailable()
        rows = {r.transaction_id: r for r in spark.table("features_test").collect()}
    finally:
        query.stop()

    assert rows["t2"].txn_count_1m == 2
    t3 = rows["t3"]
    assert t3.history_count == 2  # state from the previous micro-batch was restored
    assert t3.txn_count_5m == 2  # t1 aged out of the 5-minute window, t2 did not
    assert t3.seconds_since_prev == pytest.approx(280.0)
    assert t3.km_from_prev == pytest.approx(haversine_km(*nyc, *london))
    assert t3.implied_speed_kmh > 50_000


def test_merchant_windows_assign_each_event_to_five_sliding_windows(spark):
    from streaming.gold_sink import merchant_risk_windows

    rows = [
        (1, datetime(2026, 9, 24, 10, 2, 30), 10.0, 7, True, 0.9),
        (1, datetime(2026, 9, 24, 10, 3, 30), 30.0, 8, False, 0.1),
    ]
    gold = spark.createDataFrame(
        rows, "merchant_key long, event_ts timestamp, amount_usd double, user_key long, is_flagged boolean, "
        "fraud_score double"
    )  # fmt: skip
    out = merchant_risk_windows(gold, "5 minutes", "1 minute", "10 minutes").orderBy("window_start").collect()
    assert len(out) == 6  # union of each event's five windows
    both = [r for r in out if r.txn_count == 2]
    assert [r.window_start.strftime("%H:%M") for r in both] == ["09:59", "10:00", "10:01", "10:02"]
    assert both[0].flagged_count == 1
    assert float(both[0].total_amount_usd) == 40.0
    assert float(both[0].avg_fraud_score) == pytest.approx(0.5)


def test_enrichment_resolves_keys_and_unknown_members(spark):
    from streaming.gold_sink import Dimensions, enrich

    silver = spark.createDataFrame(
        [
            ("t1", "U1", "M1", "Paris", "FR", 48.8566, 2.3522, date(2026, 9, 24), datetime(2026, 9, 24, 10)),
            ("t2", "U404", "M404", "Atlantis", "??", 0.0, 0.0, date(2026, 9, 24), datetime(2026, 9, 24, 10)),
        ],
        "transaction_id string, user_id string, merchant_id string, city string, country_code string, "
        "location_lat double, location_lon double, event_date date, event_ts timestamp",
    )
    dims = Dimensions(
        users=spark.createDataFrame(
            [(10, "U1", 40.7128, -74.0060, date(2026, 9, 1), False)],
            "user_key long, user_id string, home_lat double, home_lon double, account_open_date date, "
            "has_chip_card boolean",
        ),
        merchants=spark.createDataFrame([(20, "M1", "5732", "HIGH")], "merchant_key long, merchant_id string, "
                                        "mcc string, category_risk_tier string"),
        locations=spark.createDataFrame([(30, "Paris", "FR")], "location_key int, city string, country_code string"),
        loaded_at=0.0,
    )  # fmt: skip
    rows = {r.transaction_id: r for r in enrich(silver, dims).collect()}
    known, unknown = rows["t1"], rows["t2"]
    assert (known.user_key, known.merchant_key, known.location_key) == (10, 20, 30)
    assert known.category_risk_tier == "HIGH" and known.has_chip_card is False
    assert known.account_age_days == 23
    assert known.date_key == 20260924
    assert known.distance_from_home_km == pytest.approx(haversine_km(40.7128, -74.0060, 48.8566, 2.3522), rel=1e-9)
    assert (unknown.user_key, unknown.merchant_key, unknown.location_key) == (-1, -1, -1)
    assert unknown.mcc == "0000" and unknown.category_risk_tier == "UNKNOWN"
    assert unknown.distance_from_home_km is None  # unknown home -> NULL, never a fabricated distance

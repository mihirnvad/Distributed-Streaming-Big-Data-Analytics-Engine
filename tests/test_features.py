"""Unit tests for the per-card stateful feature engine (pure Python, no JVM)."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from common.geo import haversine_km
from streaming.features import (
    FEATURE_INPUT_COLUMNS,
    FEATURE_NAMES,
    MIN_HISTORY_FOR_BASELINE,
    MIN_HISTORY_FOR_ZSCORE,
    RECENT_RETENTION_MS,
    UserFeatureEngine,
    UserState,
    compute_features,
)

T0 = 1_750_000_000_000  # arbitrary epoch ms
NYC = (40.7128, -74.0060)
LONDON = (51.5074, -0.1278)


def swipe(engine: UserFeatureEngine, offset_s: float, amount: float = 40.0, merchant: str = "M1", where=NYC):
    return engine.process(T0 + int(offset_s * 1000), amount, merchant, *where)


def test_first_transaction_has_no_history_features():
    f = swipe(UserFeatureEngine(), 0)
    assert f["txn_count_1m"] == f["txn_count_5m"] == 1
    assert f["seconds_since_prev"] is None and f["implied_speed_kmh"] is None
    assert f["amount_zscore"] is None and f["baseline_txn_per_min"] is None
    assert f["history_count"] == 0


def test_trailing_windows_count_only_recent_events():
    engine = UserFeatureEngine()
    for offset in (0, 30, 90, 200):
        swipe(engine, offset, merchant=f"M{offset}")
    f = swipe(engine, 310, merchant="M0")  # 0s is now 310s old -> outside 5 min
    assert f["txn_count_5m"] == 4  # 30, 90, 200 and the current one
    assert f["txn_count_1m"] == 1
    assert f["distinct_merchants_5m"] == 4
    assert f["amount_usd_5m"] == pytest.approx(160.0)


def test_small_ticket_counter_detects_card_testing_pattern():
    engine = UserFeatureEngine()
    features = [swipe(engine, i * 5, amount=1.99, merchant=f"M{i}") for i in range(6)]
    assert features[-1]["small_txn_count_5m"] == 6
    assert features[-1]["distinct_merchants_5m"] == 6
    assert features[-1]["txn_count_1m"] == 6


def test_impossible_travel_speed():
    engine = UserFeatureEngine()
    swipe(engine, 0, where=NYC)
    f = swipe(engine, 600, where=LONDON)  # NYC -> London in 10 minutes
    distance = haversine_km(*NYC, *LONDON)
    assert f["km_from_prev"] == pytest.approx(distance)
    assert f["implied_speed_kmh"] == pytest.approx(distance / (600 / 3600))
    assert f["implied_speed_kmh"] > 30_000


def test_speed_floor_prevents_division_blowups_for_same_second_swipes():
    engine = UserFeatureEngine()
    swipe(engine, 0, where=NYC)
    f = swipe(engine, 1, where=(40.72, -74.00))  # ~1 km away one second later
    assert f["implied_speed_kmh"] < 100  # elapsed time is floored at 60 s


def test_amount_zscore_uses_prior_history_only():
    engine = UserFeatureEngine()
    for i in range(MIN_HISTORY_FOR_ZSCORE):
        f = swipe(engine, i * 600, amount=40.0 + i)
        assert f["amount_zscore"] is None
    spike = swipe(engine, 10_000, amount=4_000.0)
    normal = swipe(engine, 20_000, amount=42.0)
    assert spike["amount_zscore"] > 3.0
    assert abs(normal["amount_zscore"]) < spike["amount_zscore"]


def test_baseline_rate_learned_from_interarrival_gaps():
    engine = UserFeatureEngine()
    for i in range(MIN_HISTORY_FOR_BASELINE + 1):
        f = swipe(engine, i * 120, merchant=f"M{i}")  # one swipe every 2 minutes
    assert f["baseline_txn_per_min"] == pytest.approx(0.5, rel=0.01)


def test_out_of_order_event_uses_correct_previous_swipe():
    engine = UserFeatureEngine()
    swipe(engine, 0, where=NYC)
    swipe(engine, 500, where=LONDON)
    late = swipe(engine, 250, where=NYC)  # arrives after the London swipe but happened before it
    assert late["seconds_since_prev"] == pytest.approx(250)
    assert late["km_from_prev"] == pytest.approx(0.0, abs=1e-6)
    assert engine.state.last_lat == pytest.approx(LONDON[0])  # "last" still points at newest event


def test_events_older_than_retention_are_evicted():
    engine = UserFeatureEngine()
    swipe(engine, 0, where=NYC)
    swipe(engine, RECENT_RETENTION_MS / 1000, where=LONDON)
    assert len(engine.state.recent_ts) == 1  # the NYC swipe aged out of the buffer
    late = swipe(engine, 30, where=NYC)  # older than anything retained: no reliable predecessor
    assert late["seconds_since_prev"] is None


def test_state_round_trip_and_retention():
    engine = UserFeatureEngine()
    for i in range(20):
        swipe(engine, i * 60, merchant=f"M{i}")
    newest = max(engine.state.recent_ts)
    assert all(newest - t < RECENT_RETENTION_MS for t in engine.state.recent_ts)

    restored = UserFeatureEngine(UserState.from_tuple(engine.state.to_tuple()))
    assert restored.state == engine.state
    a = swipe(engine, 20 * 60, merchant="X")
    b = swipe(restored, 20 * 60, merchant="X")
    assert a == b


def test_state_tuple_arity_matches_spark_schema():
    pytest.importorskip("pyspark")
    from streaming.schemas import USER_FEATURE_STATE_SCHEMA

    assert len(UserState().to_tuple()) == len(USER_FEATURE_STATE_SCHEMA.fields)


def test_compute_features_sorts_by_event_time_and_appends_columns():
    rows = []
    for i, offset in enumerate((120, 0, 60)):
        rows.append(
            {
                "transaction_id": f"t{i}",
                "event_ts": pd.Timestamp(T0 + offset * 1000, unit="ms"),
                "event_date": pd.Timestamp(T0, unit="ms").date(),
                "user_id": "U1",
                "card_id": "c",
                "merchant_id": f"M{i}",
                "amount": 10.0,
                "currency": "USD",
                "amount_usd": 10.0,
                "channel": "POS",
                "entry_mode": "CHIP",
                "location_lat": NYC[0],
                "location_lon": NYC[1],
                "city": "New York",
                "country_code": "US",
                "kafka_partition": 0,
                "kafka_offset": i,
                "kafka_ts": pd.Timestamp(T0, unit="ms"),
            }
        )
    out = compute_features(UserFeatureEngine(), pd.DataFrame(rows))
    assert list(out.columns) == FEATURE_INPUT_COLUMNS + FEATURE_NAMES
    assert list(out["transaction_id"]) == ["t1", "t2", "t0"]
    assert list(out["txn_count_5m"]) == [1, 2, 3]
    assert math.isclose(out["seconds_since_prev"].iloc[2], 60.0)

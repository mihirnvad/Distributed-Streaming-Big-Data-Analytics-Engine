"""Tests for the synthetic population and event simulator (pure Python)."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from common.geo import haversine_km
from common.reference import CITIES, DISPUTE_REASON_BY_CODE, MERCHANT_CATEGORIES, USD_PER_UNIT
from producer.entities import build_population
from producer.simulator import CHARGEBACKS, TRANSACTIONS, SimulationConfig, TransactionSimulator

NOW = 1_750_000_000.0


@pytest.fixture(scope="module")
def population():
    return build_population(n_users=3_000, n_merchants=800, seed=7)


def _run(sim: TransactionSimulator, ticks: int = 50, per_tick: int = 200):
    messages = []
    for i in range(ticks):
        messages += sim.generate(per_tick, NOW + i)
    messages += sim.drain()
    return messages


def test_population_is_deterministic(population):
    again = build_population(n_users=3_000, n_merchants=800, seed=7)
    assert [u.user_id for u in population.users] == [u.user_id for u in again.users]
    assert [(m.merchant_id, m.mcc, m.city_idx) for m in population.merchants] == [
        (m.merchant_id, m.mcc, m.city_idx) for m in again.merchants
    ]
    assert population.users[123].card_id == again.users[123].card_id


def test_every_city_has_every_physical_category(population):
    for city_idx in range(len(CITIES)):
        for cat in MERCHANT_CATEGORIES:
            if cat.online_share < 1.0:
                assert population.local_pool(city_idx, cat.mcc) is not None, (city_idx, cat.mcc)


def test_transaction_events_match_contract(population):
    sim = TransactionSimulator(population, population.users, SimulationConfig(malformed_rate=0), seed=1)
    for msg in _run(sim, ticks=5):
        record = json.loads(msg.value)
        if msg.stream == TRANSACTIONS:
            assert msg.key == record["user_id"]
            assert record["currency"] in USD_PER_UNIT
            assert record["amount"] > 0
            assert -90 <= record["location_lat"] <= 90 and -180 <= record["location_lon"] <= 180
            assert record["event_ts"].endswith("Z")
        else:
            assert record["reason_code"] in DISPUTE_REASON_BY_CODE


def test_simulator_is_deterministic_for_a_seed(population):
    a = _run(TransactionSimulator(population, population.users, seed=99), ticks=3)
    b = _run(TransactionSimulator(population, population.users, seed=99), ticks=3)
    assert [m.value for m in a] == [m.value for m in b]


def test_fraud_scenarios_produce_labelled_chargebacks(population):
    config = SimulationConfig(fraud_scenario_rate=0.02, friendly_fraud_rate=0.0)
    sim = TransactionSimulator(population, population.users, config, seed=3)
    messages = _run(sim)

    fraud_ids = {json.loads(m.value)["transaction_id"] for m in messages if m.kind == "fraud"}
    chargebacks = [json.loads(m.value) for m in messages if m.stream == CHARGEBACKS]
    assert fraud_ids and chargebacks
    assert {cb["transaction_id"] for cb in chargebacks} <= fraud_ids
    assert all(DISPUTE_REASON_BY_CODE[cb["reason_code"]].is_fraud for cb in chargebacks)
    # ~85% of fraud gets reported
    assert 0.7 < len(chargebacks) / len(fraud_ids) < 0.95
    scenarios = {k for k in sim.stats if k.startswith("scenario:")}
    assert scenarios == {
        "scenario:card_testing",
        "scenario:account_takeover",
        "scenario:cloned_card",
        "scenario:spending_spree",
    }


def test_cloned_card_fraud_happens_far_from_home(population):
    sim = TransactionSimulator(population, population.users[:1], seed=5)
    user = population.users[0]
    events = sim._scenario_cloned_card(user, NOW)
    genuine = events[0][1]
    home = user.home_city
    assert events[0][2] is False
    for _, txn, is_fraud in events[1:]:
        assert is_fraud
        assert txn["entry_mode"] == "SWIPE"
        assert haversine_km(home.lat, home.lon, txn["location_lat"], txn["location_lon"]) >= 2_500
        assert txn["event_ts"] > genuine["event_ts"]


def test_data_quality_chaos_is_injected(population):
    config = SimulationConfig(duplicate_rate=0.05, late_rate=0.05, malformed_rate=0.02, fraud_scenario_rate=0)
    sim = TransactionSimulator(population, population.users, config, seed=11)
    messages = _run(sim, ticks=20)
    kinds = Counter(m.kind for m in messages)
    assert kinds["duplicate"] > 0 and kinds["late"] > 0 and kinds["malformed"] > 0

    by_value = Counter(m.value for m in messages if m.stream == TRANSACTIONS)
    assert any(count > 1 for count in by_value.values()), "duplicates must be byte-identical replays"

    malformed = [m.value for m in messages if m.kind == "malformed"]
    broken = 0
    for raw in malformed:
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            broken += 1
            continue
        assert (
            record.get("amount", 1) <= 0
            or "merchant_id" not in record
            or record.get("currency") == "XXX"
            or abs(record.get("location_lat", 0)) > 90
            or record.get("event_ts") == "not-a-timestamp"
        )
    assert broken > 0

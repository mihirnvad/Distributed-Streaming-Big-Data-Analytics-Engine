"""Schema-contract tests: producer <-> Spark <-> feature engine, plus parsing/quarantine."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from producer.entities import build_population
from producer.simulator import CHARGEBACKS, TRANSACTIONS, SimulationConfig, TransactionSimulator
from streaming import features
from streaming.schemas import (
    BRONZE_SCHEMA,
    CHARGEBACK_EVENT_SCHEMA,
    GOLD_TRANSACTION_SCHEMA,
    QUARANTINE_SCHEMA,
    SILVER_CHARGEBACK_SCHEMA,
    SILVER_TRANSACTION_SCHEMA,
    TRANSACTION_EVENT_SCHEMA,
    USER_FEATURE_ENGINE_OUTPUT_SCHEMA,
    USER_FEATURE_FIELDS,
    USER_FEATURE_OUTPUT_SCHEMA,
)

KAFKA_TS = datetime(2026, 9, 24, 12, 0, 5, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- pure contract checks


def test_feature_engine_contract_matches_spark_schemas():
    assert [f.name for f in SILVER_TRANSACTION_SCHEMA.fields] == features.FEATURE_INPUT_COLUMNS
    assert [f.name for f in USER_FEATURE_FIELDS] == features.FEATURE_NAMES
    assert [
        f.name for f in USER_FEATURE_OUTPUT_SCHEMA.fields
    ] == features.FEATURE_INPUT_COLUMNS + features.FEATURE_NAMES
    assert [f.name for f in USER_FEATURE_ENGINE_OUTPUT_SCHEMA.fields] == features.ENGINE_OUTPUT_COLUMNS


def test_gold_schema_has_unique_columns():
    names = [f.name for f in GOLD_TRANSACTION_SCHEMA.fields]
    assert len(names) == len(set(names))


def test_producer_payloads_match_declared_wire_schema():
    population = build_population(500, 400, seed=1)
    sim = TransactionSimulator(population, population.users, SimulationConfig(fraud_scenario_rate=0.05), seed=1)
    messages = []
    for i in range(20):
        messages += sim.generate(100, 1_750_000_000 + i)
    messages += sim.drain()
    txn_fields = {f.name for f in TRANSACTION_EVENT_SCHEMA.fields}
    cb_fields = {f.name for f in CHARGEBACK_EVENT_SCHEMA.fields}
    seen = {TRANSACTIONS: 0, CHARGEBACKS: 0}
    for msg in messages:
        if msg.kind == "malformed":
            continue
        record = json.loads(msg.value)
        expected = txn_fields if msg.stream == TRANSACTIONS else cb_fields
        assert set(record) == expected, set(record) ^ expected
        seen[msg.stream] += 1
    assert seen[TRANSACTIONS] > 0 and seen[CHARGEBACKS] > 0


# --------------------------------------------------------------------------- parsing & quarantine (Spark)


def _bronze(spark, payloads: list[str | dict], topic: str = "payments.transactions"):
    rows = []
    for i, p in enumerate(payloads):
        raw = p if isinstance(p, str) else json.dumps(p)
        rows.append(("U1", raw, topic, 0, i, KAFKA_TS, KAFKA_TS, KAFKA_TS.date(), KAFKA_TS.hour))
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def _txn(**overrides) -> dict:
    base = {
        "schema_version": 1,
        "transaction_id": "7f1c7c2e-0000-4000-8000-000000000001",
        "event_ts": "2026-09-24T12:00:00.123Z",
        "user_id": "U0000001",
        "card_id": "tok_abc",
        "merchant_id": "M000001",
        "amount": 100.0,
        "currency": "EUR",
        "channel": "POS",
        "entry_mode": "CHIP",
        "location_lat": 48.85,
        "location_lon": 2.35,
        "city": "Paris",
        "country_code": "FR",
    }
    base.update(overrides)
    return base


@pytest.mark.spark
def test_valid_transaction_is_parsed_and_normalised(spark):
    from streaming.silver_transforms import parse_transactions, valid_transactions

    parsed = parse_transactions(_bronze(spark, [_txn()]))
    assert parsed.select("dq_errors").first()[0] == []
    silver = valid_transactions(parsed)
    assert silver.columns == [f.name for f in SILVER_TRANSACTION_SCHEMA.fields]
    row = silver.first()
    assert row.event_ts == datetime(2026, 9, 24, 12, 0, 0, 123000)  # session TZ is UTC
    assert row.amount == Decimal("100.00")
    assert row.amount_usd == Decimal("109.00")  # EUR -> USD at 1.09
    assert str(row.event_date) == "2026-09-24"


@pytest.mark.spark
@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ('{"schema_version":1,"transaction_id":"x","amount":', "MALFORMED_JSON"),
        (_txn(amount=-5), "INVALID_AMOUNT"),
        ({k: v for k, v in _txn().items() if k != "merchant_id"}, "MISSING_REQUIRED_FIELD"),
        (_txn(currency="XXX"), "UNKNOWN_CURRENCY"),
        (_txn(location_lat=123.4), "INVALID_COORDINATES"),
        (_txn(event_ts="not-a-timestamp"), "INVALID_TIMESTAMP"),
        (_txn(event_ts="2026-09-24T13:00:00Z"), "FUTURE_TIMESTAMP"),
        (_txn(schema_version=2), "UNSUPPORTED_SCHEMA_VERSION"),
        (_txn(channel="TELEPATHY"), "INVALID_CHANNEL"),
    ],
)
def test_bad_records_are_quarantined_with_reason(spark, payload, expected):
    from streaming.silver_transforms import parse_transactions, quarantined_records, valid_transactions

    parsed = parse_transactions(_bronze(spark, [payload]))
    assert valid_transactions(parsed).count() == 0
    quarantined = quarantined_records(parsed)
    assert quarantined.columns == [f.name for f in QUARANTINE_SCHEMA.fields]
    errors = quarantined.first().dq_errors
    assert expected in errors, errors


@pytest.mark.spark
def test_chargeback_parsing(spark):
    from streaming.silver_transforms import parse_chargebacks, valid_chargebacks

    good = {
        "schema_version": 1,
        "chargeback_id": "cb-1",
        "transaction_id": "t-1",
        "user_id": "U1",
        "reason_code": "10.4",
        "amount": 20.0,
        "currency": "GBP",
        "reported_ts": "2026-09-24T12:03:00Z",
    }
    bad = dict(good, chargeback_id="cb-2", reason_code="99.9")
    parsed = parse_chargebacks(_bronze(spark, [good, bad], topic="payments.chargebacks"))
    valid = valid_chargebacks(parsed)
    assert valid.columns == [f.name for f in SILVER_CHARGEBACK_SCHEMA.fields]
    rows = valid.collect()
    assert [r.chargeback_id for r in rows] == ["cb-1"]
    assert rows[0].amount_usd == Decimal("25.40")
    rejected = parsed.where("size(dq_errors) > 0").first()
    assert "UNKNOWN_REASON_CODE" in rejected.dq_errors

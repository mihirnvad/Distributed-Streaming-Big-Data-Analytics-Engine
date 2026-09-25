"""Rule-engine tests: configuration validation and per-rule behaviour."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from streaming.scoring import REGISTERED_RULES, apply_rules, load_rules

RULES_PATH = Path(__file__).resolve().parents[1] / "config" / "fraud_rules.yaml"

FEATURE_DDL = (
    "id string, txn_count_1m int, txn_count_5m int, baseline_txn_per_min double, small_txn_count_5m int, "
    "distinct_merchants_5m int, amount_zscore double, implied_speed_kmh double, km_from_prev double, "
    "category_risk_tier string, amount_usd double, channel string, distance_from_home_km double, "
    "entry_mode string, has_chip_card boolean, account_age_days int"
)
BENIGN = dict(
    txn_count_1m=1, txn_count_5m=2, baseline_txn_per_min=0.5, small_txn_count_5m=0, distinct_merchants_5m=2,
    amount_zscore=0.2, implied_speed_kmh=5.0, km_from_prev=1.0, category_risk_tier="LOW", amount_usd=42.0,
    channel="POS", distance_from_home_km=3.0, entry_mode="CHIP", has_chip_card=True, account_age_days=900,
)  # fmt: skip
COLUMNS = [c.split()[0] for c in FEATURE_DDL.split(", ")]


def test_config_loads_and_every_rule_is_registered():
    rules = load_rules(RULES_PATH)
    assert {r.name for r in rules.rules} == REGISTERED_RULES
    assert [name for name, _ in rules.risk_levels] == ["CRITICAL", "HIGH", "MEDIUM"]
    assert 0 < rules.alert_threshold < 1


def test_config_rejects_unknown_rules_and_bad_weights(tmp_path):
    base = RULES_PATH.read_text(encoding="utf-8")
    unknown = tmp_path / "unknown.yaml"
    unknown.write_text(base.replace("CARD_TESTING:", "CARD_TESTNG:"), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown rule"):
        load_rules(unknown)
    heavy = tmp_path / "heavy.yaml"
    heavy.write_text(base.replace("weight: 0.70", "weight: 1.5"), encoding="utf-8")
    with pytest.raises(ValueError, match="weight"):
        load_rules(heavy)


@pytest.fixture(scope="module")
def scored(spark):
    cases = {
        "benign": {},
        "cloned_card": dict(
            implied_speed_kmh=22_000.0, km_from_prev=5_500.0, entry_mode="SWIPE", distance_from_home_km=5_500.0
        ),
        "card_testing": dict(small_txn_count_5m=5, distinct_merchants_5m=5, amount_usd=1.5, channel="ECOM"),
        "velocity_cold_start": dict(txn_count_1m=6, baseline_txn_per_min=None),
        "velocity_heavy_user": dict(txn_count_1m=6, txn_count_5m=9, baseline_txn_per_min=3.0),
        "amount_spike_high_risk": dict(amount_zscore=4.2, amount_usd=1_800.0, category_risk_tier="HIGH"),
        "no_history": dict(amount_zscore=None, implied_speed_kmh=None, km_from_prev=None, baseline_txn_per_min=None),
        "new_account": dict(account_age_days=5, amount_usd=900.0),
    }
    rows = [(name, *[{**BENIGN, **overrides}[c] for c in COLUMNS[1:]]) for name, overrides in cases.items()]
    df = spark.createDataFrame(rows, FEATURE_DDL)
    return {r.id: r for r in apply_rules(df, load_rules(RULES_PATH)).collect()}


@pytest.mark.spark
def test_benign_transaction_scores_zero(scored):
    row = scored["benign"]
    assert (row.fraud_score, row.risk_level, row.is_flagged, row.reason_codes) == (0.0, "LOW", False, [])


@pytest.mark.spark
def test_signals_combine_with_noisy_or(scored):
    row = scored["cloned_card"]
    assert set(row.reason_codes) == {"IMPOSSIBLE_TRAVEL", "MAGSTRIPE_ON_CHIP_CARD", "FAR_FROM_HOME_CARD_PRESENT"}
    assert row.fraud_score == pytest.approx(1 - 0.25 * 0.75 * 0.80, abs=1e-4)
    assert row.risk_level == "CRITICAL" and row.is_flagged


@pytest.mark.spark
def test_card_testing_rule(scored):
    row = scored["card_testing"]
    assert row.reason_codes == ["CARD_TESTING"]
    assert row.risk_level == "HIGH" and row.is_flagged


@pytest.mark.spark
def test_velocity_is_relative_to_the_cards_own_baseline(scored):
    assert scored["velocity_cold_start"].reason_codes == ["VELOCITY_BURST"]
    assert scored["velocity_heavy_user"].reason_codes == []  # 6/min is normal for a 3/min card


@pytest.mark.spark
def test_missing_history_never_fires_rules(scored):
    assert scored["no_history"].reason_codes == []


@pytest.mark.spark
def test_amount_spike_in_high_risk_category(scored):
    row = scored["amount_spike_high_risk"]
    assert set(row.reason_codes) == {"AMOUNT_SPIKE", "HIGH_RISK_MCC_LARGE_TICKET"}
    assert row.fraud_score == pytest.approx(1 - 0.55 * 0.70, abs=1e-4)
    assert row.is_flagged


@pytest.mark.spark
def test_new_account_rule_alone_does_not_alert(scored):
    row = scored["new_account"]
    assert row.reason_codes == ["NEW_ACCOUNT_HIGH_VALUE"]
    assert row.risk_level == "LOW" and not row.is_flagged

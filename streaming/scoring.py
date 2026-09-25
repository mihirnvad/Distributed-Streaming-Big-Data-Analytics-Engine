"""Explainable, configuration-driven fraud rule engine.

Rules are Spark column expressions, so scoring runs fully vectorised inside the
JVM (no Python UDF). Every scored transaction carries the list of rules that
fired (``reason_codes``) - the explanation an analyst or a regulator asks for.
"""

from __future__ import annotations

import functools
import operator
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

RuleBuilder = Callable[[dict[str, Any]], Column]


@dataclass(frozen=True)
class Rule:
    name: str
    weight: float
    description: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


@dataclass(frozen=True)
class RuleSet:
    version: str
    alert_threshold: float
    risk_levels: tuple[tuple[str, float], ...]  # sorted by threshold, descending
    rules: tuple[Rule, ...]

    @property
    def active_rules(self) -> tuple[Rule, ...]:
        return tuple(r for r in self.rules if r.enabled)


# --------------------------------------------------------------------------- rule definitions

_RULES: dict[str, RuleBuilder] = {}


def _rule(name: str) -> Callable[[RuleBuilder], RuleBuilder]:
    def register(fn: RuleBuilder) -> RuleBuilder:
        _RULES[name] = fn
        return fn

    return register


@_rule("VELOCITY_BURST")
def _velocity_burst(p: dict[str, Any]) -> Column:
    baseline = F.col("baseline_txn_per_min")
    c1, c5 = F.col("txn_count_1m"), F.col("txn_count_5m")
    mult = F.lit(float(p["baseline_multiplier"]))
    with_baseline = ((c1 >= p["min_count_1m"]) & (c1 >= mult * baseline)) | (
        (c5 >= p["min_count_5m"]) & (c5 >= mult * baseline * 5)
    )
    cold_start = (c1 >= p["cold_start_min_count_1m"]) | (c5 >= p["cold_start_min_count_5m"])
    return F.when(baseline.isNull(), cold_start).otherwise(with_baseline)


@_rule("CARD_TESTING")
def _card_testing(p: dict[str, Any]) -> Column:
    return (F.col("small_txn_count_5m") >= p["min_small_txns_5m"]) & (
        F.col("distinct_merchants_5m") >= p["min_distinct_merchants_5m"]
    )


@_rule("AMOUNT_SPIKE")
def _amount_spike(p: dict[str, Any]) -> Column:
    return F.col("amount_zscore") >= p["min_zscore"]


@_rule("IMPOSSIBLE_TRAVEL")
def _impossible_travel(p: dict[str, Any]) -> Column:
    return (F.col("implied_speed_kmh") > p["max_speed_kmh"]) & (F.col("km_from_prev") >= p["min_distance_km"])


@_rule("HIGH_RISK_MCC_LARGE_TICKET")
def _high_risk_mcc(p: dict[str, Any]) -> Column:
    return (F.col("category_risk_tier") == "HIGH") & (F.col("amount_usd") >= p["min_amount_usd"])


@_rule("FAR_FROM_HOME_CARD_PRESENT")
def _far_from_home(p: dict[str, Any]) -> Column:
    return (F.col("channel") != "ECOM") & (F.col("distance_from_home_km") >= p["min_distance_km"])


@_rule("MAGSTRIPE_ON_CHIP_CARD")
def _magstripe(_: dict[str, Any]) -> Column:
    return (F.col("entry_mode") == "SWIPE") & F.col("has_chip_card")


@_rule("NEW_ACCOUNT_HIGH_VALUE")
def _new_account(p: dict[str, Any]) -> Column:
    return (F.col("account_age_days") <= p["max_account_age_days"]) & (F.col("amount_usd") >= p["min_amount_usd"])


REGISTERED_RULES = frozenset(_RULES)


# --------------------------------------------------------------------------- loading & scoring


def load_rules(path: str | Path) -> RuleSet:
    """Parse and validate the YAML rule configuration."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    rules = []
    for name, body in (raw.get("rules") or {}).items():
        if name not in _RULES:
            raise ValueError(f"unknown rule '{name}' in {path}; known rules: {sorted(_RULES)}")
        weight = float(body["weight"])
        if not 0.0 < weight < 1.0:
            raise ValueError(f"rule '{name}' weight must be in (0, 1), got {weight}")
        rules.append(
            Rule(
                name=name,
                weight=weight,
                description=str(body.get("description", "")).strip(),
                params=dict(body.get("params") or {}),
                enabled=bool(body.get("enabled", True)),
            )
        )
    levels = tuple(sorted(((k, float(v)) for k, v in raw["risk_levels"].items()), key=lambda kv: -kv[1]))
    return RuleSet(
        version=str(raw["version"]),
        alert_threshold=float(raw["alert_threshold"]),
        risk_levels=levels,
        rules=tuple(rules),
    )


def rule_hits(rules: RuleSet) -> dict[str, Column]:
    """Boolean column per active rule; NULL inputs (e.g. no history yet) never fire a rule."""
    return {r.name: F.coalesce(_RULES[r.name](r.params), F.lit(False)) for r in rules.active_rules}


def apply_rules(df: DataFrame, rules: RuleSet) -> DataFrame:
    """Append ``fraud_score``, ``reason_codes``, ``risk_level``, ``is_flagged`` and ``rules_version``."""
    hits = rule_hits(rules)
    active = rules.active_rules
    if not active:
        survival: Column = F.lit(1.0)
        reasons: Column = F.array().cast("array<string>")
    else:
        survival = functools.reduce(
            operator.mul,
            [F.when(hits[r.name], F.lit(1.0 - r.weight)).otherwise(F.lit(1.0)) for r in active],
        )
        reasons = F.array_compact(F.array(*[F.when(hits[r.name], F.lit(r.name)) for r in active]))

    score = F.round(F.lit(1.0) - survival, 4)
    level: Column = F.lit("LOW")
    for name, threshold in reversed(rules.risk_levels):  # build the CASE from the lowest band up
        level = F.when(F.col("fraud_score") >= threshold, F.lit(name)).otherwise(level)

    return (
        df.withColumn("fraud_score", score)
        .withColumn("reason_codes", reasons)
        .withColumn("risk_level", level)
        .withColumn("is_flagged", F.col("fraud_score") >= F.lit(rules.alert_threshold))
        .withColumn("rules_version", F.lit(rules.version))
    )

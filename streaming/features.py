"""Per-card real-time feature engine (arbitrary stateful stream processing).

Window aggregations answer "how many swipes did this card make between 10:00 and
10:05?", but fraud scoring needs "how many swipes did this card make in the five
minutes *before this swipe*?" - a per-event trailing window - plus sequence
features such as the implied travel speed since the previous swipe. Those need
per-key state that survives across micro-batches, so the gold job runs this
engine inside ``applyInPandasWithState`` keyed by ``user_id``.

The core (:class:`UserFeatureEngine`) is plain Python with no Spark dependency so
it is unit-testable in milliseconds; :func:`update_user_features` is the thin
adapter Spark calls for every key in every micro-batch.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from common.geo import haversine_km, implied_speed_kmh

WINDOW_1M_MS = 60_000
WINDOW_5M_MS = 5 * 60_000
# Keep enough history to evaluate the 5-minute window for events that arrive up to
# five minutes out of order.
RECENT_RETENTION_MS = 10 * 60_000
MAX_RECENT_EVENTS = 256
SMALL_TICKET_USD = 5.0
MIN_HISTORY_FOR_ZSCORE = 5
MIN_HISTORY_FOR_BASELINE = 10
ZSCORE_STD_FLOOR = 0.25  # on log(amount); stops tiny variance producing huge z-scores
GAP_EWMA_ALPHA = 0.1
GAP_CLAMP_S = (1.0, 3_600.0)

# Column contracts shared with streaming/schemas.py (kept in sync by tests/test_schemas.py).
# Declared here as plain lists so this module stays importable without Spark.
FEATURE_INPUT_COLUMNS = [
    "transaction_id", "event_ts", "event_date", "user_id", "card_id", "merchant_id", "amount", "currency",
    "amount_usd", "channel", "entry_mode", "location_lat", "location_lon", "city", "country_code",
    "kafka_partition", "kafka_offset", "kafka_ts",
]  # fmt: skip
FEATURE_NAMES = [
    "txn_count_1m", "txn_count_5m", "amount_usd_5m", "distinct_merchants_5m", "small_txn_count_5m",
    "seconds_since_prev", "km_from_prev", "implied_speed_kmh", "amount_zscore", "baseline_txn_per_min",
    "history_count",
]  # fmt: skip

# What actually crosses the JVM/Python boundary. Spark calls the stateful function once
# per card per micro-batch and each column costs a pandas conversion per call, so the
# operator receives only what the engine reads plus one opaque ``passthrough`` string
# (the rest of the silver row, packed/unpacked by the JVM), and returns the features as
# a single array column aligned with FEATURE_NAMES (see streaming/gold_sink.py).
ENGINE_INPUT_COLUMNS = [
    "user_id", "transaction_id", "event_ts", "amount_usd", "merchant_id", "location_lat", "location_lon", "passthrough",
]  # fmt: skip
ENGINE_OUTPUT_COLUMNS = ["transaction_id", "event_ts", "passthrough", "features"]


@dataclass
class UserState:
    """Serializable per-card state (mirrors ``USER_FEATURE_STATE_SCHEMA``)."""

    recent_ts: list[int] = field(default_factory=list)
    recent_amount: list[float] = field(default_factory=list)
    recent_merchant: list[str] = field(default_factory=list)
    recent_lat: list[float] = field(default_factory=list)
    recent_lon: list[float] = field(default_factory=list)
    last_ts: int | None = None
    last_lat: float | None = None
    last_lon: float | None = None
    n_obs: int = 0
    mean_log_amount: float = 0.0
    m2_log_amount: float = 0.0
    ewma_gap_s: float | None = None

    def to_tuple(self) -> tuple:
        return (
            list(self.recent_ts),
            list(self.recent_amount),
            list(self.recent_merchant),
            list(self.recent_lat),
            list(self.recent_lon),
            self.last_ts,
            self.last_lat,
            self.last_lon,
            self.n_obs,
            self.mean_log_amount,
            self.m2_log_amount,
            self.ewma_gap_s,
        )

    @classmethod
    def from_tuple(cls, values: tuple | Any) -> UserState:
        (rts, ramt, rmer, rlat, rlon, last_ts, last_lat, last_lon, n_obs, mean, m2, ewma) = tuple(values)
        return cls(
            recent_ts=[int(v) for v in (rts or [])],
            recent_amount=[float(v) for v in (ramt or [])],
            recent_merchant=[str(v) for v in (rmer or [])],
            recent_lat=[float(v) for v in (rlat or [])],
            recent_lon=[float(v) for v in (rlon or [])],
            last_ts=None if last_ts is None else int(last_ts),
            last_lat=last_lat,
            last_lon=last_lon,
            n_obs=int(n_obs or 0),
            mean_log_amount=float(mean or 0.0),
            m2_log_amount=float(m2 or 0.0),
            ewma_gap_s=None if ewma is None else float(ewma),
        )


class UserFeatureEngine:
    """Computes trailing-window, sequence and behavioural-baseline features for one card."""

    def __init__(self, state: UserState | None = None) -> None:
        self.state = state or UserState()

    def process(self, ts_ms: int, amount_usd: float, merchant_id: str, lat: float, lon: float) -> dict[str, Any]:
        """Return features for one transaction and fold it into the state.

        Counts include the current transaction; baselines (z-score, expected rate)
        use only *prior* history so the transaction cannot vouch for itself.
        """
        s = self.state

        # ---- trailing windows over (ts - window, ts], current transaction included
        count_1m = count_5m = small_5m = 1
        amount_5m = amount_usd
        merchants_5m = {merchant_id}
        if amount_usd >= SMALL_TICKET_USD:
            small_5m = 0
        prev_idx = -1
        for i, t in enumerate(s.recent_ts):
            if t > ts_ms:
                continue  # a later event already seen (current one is out of order)
            if prev_idx < 0 or t >= s.recent_ts[prev_idx]:
                prev_idx = i
            age = ts_ms - t
            if age < WINDOW_5M_MS:
                count_5m += 1
                amount_5m += s.recent_amount[i]
                merchants_5m.add(s.recent_merchant[i])
                if s.recent_amount[i] < SMALL_TICKET_USD:
                    small_5m += 1
                if age < WINDOW_1M_MS:
                    count_1m += 1

        # ---- previous swipe: newest earlier event in the buffer, else the long-lived "last" marker
        prev: tuple[int, float, float] | None = None
        if prev_idx >= 0:
            prev = (s.recent_ts[prev_idx], s.recent_lat[prev_idx], s.recent_lon[prev_idx])
        elif s.last_ts is not None and s.last_ts <= ts_ms and s.last_lat is not None and s.last_lon is not None:
            prev = (s.last_ts, s.last_lat, s.last_lon)

        seconds_since_prev = km_from_prev = speed = None
        if prev is not None:
            seconds_since_prev = (ts_ms - prev[0]) / 1000.0
            km_from_prev = haversine_km(prev[1], prev[2], lat, lon)
            speed = implied_speed_kmh(km_from_prev, seconds_since_prev)

        # ---- behavioural baselines from prior history only
        log_amount = math.log(max(amount_usd, 0.01))
        zscore = None
        if s.n_obs >= MIN_HISTORY_FOR_ZSCORE:
            std = max(math.sqrt(s.m2_log_amount / (s.n_obs - 1)), ZSCORE_STD_FLOOR)
            zscore = (log_amount - s.mean_log_amount) / std
        baseline_per_min = None
        if s.n_obs >= MIN_HISTORY_FOR_BASELINE and s.ewma_gap_s:
            baseline_per_min = 60.0 / s.ewma_gap_s

        features = {
            "txn_count_1m": count_1m,
            "txn_count_5m": count_5m,
            "amount_usd_5m": round(amount_5m, 2),
            "distinct_merchants_5m": len(merchants_5m),
            "small_txn_count_5m": small_5m,
            "seconds_since_prev": seconds_since_prev,
            "km_from_prev": km_from_prev,
            "implied_speed_kmh": speed,
            "amount_zscore": zscore,
            "baseline_txn_per_min": baseline_per_min,
            "history_count": s.n_obs,
        }
        self._update(ts_ms, amount_usd, merchant_id, lat, lon, log_amount, seconds_since_prev)
        return features

    def _update(
        self,
        ts_ms: int,
        amount_usd: float,
        merchant_id: str,
        lat: float,
        lon: float,
        log_amount: float,
        seconds_since_prev: float | None,
    ) -> None:
        s = self.state
        s.recent_ts.append(ts_ms)
        s.recent_amount.append(amount_usd)
        s.recent_merchant.append(merchant_id)
        s.recent_lat.append(lat)
        s.recent_lon.append(lon)

        newest = max(s.recent_ts)
        keep = [i for i, t in enumerate(s.recent_ts) if newest - t < RECENT_RETENTION_MS][-MAX_RECENT_EVENTS:]
        if len(keep) != len(s.recent_ts):
            s.recent_ts = [s.recent_ts[i] for i in keep]
            s.recent_amount = [s.recent_amount[i] for i in keep]
            s.recent_merchant = [s.recent_merchant[i] for i in keep]
            s.recent_lat = [s.recent_lat[i] for i in keep]
            s.recent_lon = [s.recent_lon[i] for i in keep]

        if s.last_ts is None or ts_ms >= s.last_ts:
            s.last_ts, s.last_lat, s.last_lon = ts_ms, lat, lon

        # Welford's online mean/variance of log(amount).
        s.n_obs += 1
        delta = log_amount - s.mean_log_amount
        s.mean_log_amount += delta / s.n_obs
        s.m2_log_amount += delta * (log_amount - s.mean_log_amount)

        if seconds_since_prev is not None:
            gap = min(max(seconds_since_prev, GAP_CLAMP_S[0]), GAP_CLAMP_S[1])
            s.ewma_gap_s = gap if s.ewma_gap_s is None else GAP_EWMA_ALPHA * gap + (1 - GAP_EWMA_ALPHA) * s.ewma_gap_s


def _epoch_ms(series: pd.Series) -> np.ndarray:
    """Timestamp column -> int64 epoch milliseconds, independent of pandas' datetime unit."""
    return series.to_numpy(dtype="datetime64[ms]").astype("int64")


def compute_features(engine: UserFeatureEngine, pdf: pd.DataFrame) -> pd.DataFrame:
    """Run the engine over one card's rows in event-time order.

    Input has ENGINE_INPUT_COLUMNS; output has ENGINE_OUTPUT_COLUMNS, where ``features``
    holds one list per row aligned with FEATURE_NAMES. Most cards have a single row per
    micro-batch, so the one-row path skips the sort.
    """
    if len(pdf) > 1:
        pdf = pdf.sort_values(["event_ts", "transaction_id"], kind="stable")
    features = []
    for t, a, m, la, lo in zip(
        _epoch_ms(pdf["event_ts"]),
        pdf["amount_usd"].to_numpy(dtype="float64"),
        pdf["merchant_id"].tolist(),
        pdf["location_lat"].to_numpy(dtype="float64"),
        pdf["location_lon"].to_numpy(dtype="float64"),
        strict=True,
    ):
        f = engine.process(int(t), float(a), str(m), float(la), float(lo))
        features.append([f[name] for name in FEATURE_NAMES])
    return pd.DataFrame(
        {
            "transaction_id": pdf["transaction_id"].to_numpy(),
            "event_ts": pdf["event_ts"].to_numpy(),
            "passthrough": pdf["passthrough"].to_numpy(),
            "features": features,
        }
    )


def make_state_function(idle_timeout_ms: int):
    """Build the function passed to ``groupBy(...).applyInPandasWithState``."""

    def update_user_features(key: tuple, pdf_iter: Iterator[pd.DataFrame], state) -> Iterator[pd.DataFrame]:
        if state.hasTimedOut:
            # No activity for idle_timeout of event time: evict the card's state.
            state.remove()
            return
        engine = UserFeatureEngine(UserState.from_tuple(state.get) if state.exists else None)
        for pdf in pdf_iter:
            if not pdf.empty:
                yield compute_features(engine, pdf)
        state.update(engine.state.to_tuple())
        anchor = max(engine.state.last_ts or 0, state.getCurrentWatermarkMs())
        state.setTimeoutTimestamp(anchor + idle_timeout_ms)

    return update_user_features

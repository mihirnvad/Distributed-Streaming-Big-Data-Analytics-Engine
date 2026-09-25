"""Realistic card-payment event simulator.

Besides ordinary traffic, the simulator injects the patterns a production fraud
pipeline has to cope with:

Fraud scenarios (labelled later by delayed chargebacks)
    * card testing       - bursts of micro-transactions at digital merchants, then a cash-out
    * account takeover   - large card-not-present purchases from a foreign IP location
    * cloned card        - magstripe swipes in a far-away city minutes after a genuine swipe
    * spending spree     - a stolen physical card tapped at many local shops in minutes

Legitimate behaviour that looks suspicious (the source of false positives)
    * big-ticket purchases, travel, "friendly fraud" disputes on genuine purchases

Data-quality chaos
    * duplicates (producer retries), late / out-of-order events, events arriving
      after the watermark, and malformed payloads that must be quarantined
"""

from __future__ import annotations

import heapq
import json
import math
import random
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import accumulate

from common.geo import haversine_km
from common.reference import (
    CATEGORY_BY_MCC,
    CITIES,
    MERCHANT_CATEGORIES,
    USD_PER_UNIT,
    ZERO_DECIMAL_CURRENCIES,
    City,
)
from producer.entities import Merchant, Population, User

SCHEMA_VERSION = 1
TRANSACTIONS = "transactions"
CHARGEBACKS = "chargebacks"

_CARD_TESTING_MCCS = ("5815", "4899", "5999")
_CASH_OUT_MCCS = ("5732", "6051", "5944", "7995")
_SPREE_MCCS = ("5411", "5541", "5912", "5691", "5732", "5814", "5311")
_CLONE_MCCS = ("5732", "5944", "5311", "6011")
_BIG_TICKET_MCCS = ("5732", "5944", "4511", "7011")


@dataclass(frozen=True)
class SimulationConfig:
    fraud_scenario_rate: float = 0.0006  # chance each normal event also launches a fraud scenario
    legit_big_ticket_rate: float = 0.002
    legit_travel_rate: float = 0.0003
    duplicate_rate: float = 0.004
    late_rate: float = 0.01
    late_delay_s: tuple[float, float] = (30.0, 300.0)
    very_late_rate: float = 0.0005
    very_late_delay_s: tuple[float, float] = (720.0, 1200.0)  # beyond the 10-minute watermark
    malformed_rate: float = 0.0005
    chargeback_report_rate: float = 0.85  # share of fraud that the cardholder eventually disputes
    chargeback_delay_s: tuple[float, float] = (30.0, 180.0)  # compressed from the real-world 5-60 days
    friendly_fraud_rate: float = 0.0005


@dataclass(frozen=True, slots=True)
class OutboundMessage:
    stream: str  # TRANSACTIONS | CHARGEBACKS
    key: str
    value: bytes
    kind: str  # used only for producer-side statistics


@dataclass(order=True)
class _Scheduled:
    due: float
    seq: int
    message: OutboundMessage = field(compare=False)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _to_local(amount_usd: float, currency: str) -> float:
    local = amount_usd / USD_PER_UNIT[currency]
    return float(round(local)) if currency in ZERO_DECIMAL_CURRENCIES else round(local, 2)


class TransactionSimulator:
    """Generates transaction and chargeback messages for a shard of cardholders.

    The simulator is single-threaded and deterministic for a given seed; the
    producer runs one instance per worker process, each owning a disjoint shard
    of users so per-card event ordering is preserved.
    """

    def __init__(
        self,
        population: Population,
        users: list[User],
        config: SimulationConfig | None = None,
        seed: int = 0,
    ) -> None:
        if not users:
            raise ValueError("simulator needs at least one user")
        self.population = population
        self.users = users
        self.config = config or SimulationConfig()
        self.rng = random.Random(seed)
        self.stats: Counter[str] = Counter()
        self._user_cum = list(accumulate(u.activity_weight for u in users))
        self._category_cum = list(accumulate(c.traffic_share for c in MERCHANT_CATEGORIES))
        self._queue: list[_Scheduled] = []
        self._seq = 0

    # ------------------------------------------------------------------ public API

    def generate(self, n_new: int, now: float) -> list[OutboundMessage]:
        """Create ``n_new`` fresh activity events and release everything that is due."""
        out: list[OutboundMessage] = []
        cfg = self.config
        rng = self.rng
        for user in rng.choices(self.users, cum_weights=self._user_cum, k=n_new):
            roll = rng.random()
            if roll < cfg.fraud_scenario_rate:
                self._launch_fraud_scenario(user, now)
                continue
            roll -= cfg.fraud_scenario_rate
            if roll < cfg.legit_big_ticket_rate:
                txn = self._big_ticket_transaction(user, now)
            elif roll < cfg.legit_big_ticket_rate + cfg.legit_travel_rate:
                txn = self._travel_transaction(user, now)
            else:
                txn = self._normal_transaction(user, now)
            self._emit_with_chaos(txn, now, out)
        out.extend(self._release_due(now))
        return out

    def drain(self) -> list[OutboundMessage]:
        """Release every scheduled message regardless of due time (used on shutdown/tests)."""
        return self._release_due(math.inf)

    @property
    def pending(self) -> int:
        return len(self._queue)

    # ------------------------------------------------------------------ scheduling

    def _schedule(self, due: float, message: OutboundMessage) -> None:
        self._seq += 1
        heapq.heappush(self._queue, _Scheduled(due, self._seq, message))

    def _release_due(self, now: float) -> list[OutboundMessage]:
        released = []
        while self._queue and self._queue[0].due <= now:
            msg = heapq.heappop(self._queue).message
            self.stats[msg.kind] += 1
            released.append(msg)
        return released

    def _emit(self, txn: dict, kind: str, out: list[OutboundMessage]) -> None:
        self.stats[kind] += 1
        out.append(OutboundMessage(TRANSACTIONS, txn["user_id"], _encode(txn), kind))

    def _emit_with_chaos(self, txn: dict, now: float, out: list[OutboundMessage]) -> None:
        cfg = self.config
        rng = self.rng
        roll = rng.random()
        if roll < cfg.late_rate:
            # Event happened "now" but reaches Kafka minutes later (mobile offline, batch acquirer...).
            delay = rng.uniform(*cfg.late_delay_s)
            self._schedule(now + delay, OutboundMessage(TRANSACTIONS, txn["user_id"], _encode(txn), "late"))
        elif roll < cfg.late_rate + cfg.very_late_rate:
            delay = rng.uniform(*cfg.very_late_delay_s)
            self._schedule(now + delay, OutboundMessage(TRANSACTIONS, txn["user_id"], _encode(txn), "very_late"))
        else:
            self._emit(txn, "normal", out)

        if rng.random() < cfg.duplicate_rate:
            # At-least-once delivery upstream: the exact same payload shows up again.
            self._schedule(
                now + rng.uniform(0.1, 5.0),
                OutboundMessage(TRANSACTIONS, txn["user_id"], _encode(txn), "duplicate"),
            )
        if rng.random() < cfg.malformed_rate:
            self._emit_malformed(txn, out)
        if rng.random() < cfg.friendly_fraud_rate:
            self._schedule_chargeback(txn, now, friendly=True)

    # ------------------------------------------------------------------ transaction builders

    def _transaction(
        self,
        user: User,
        merchant: Merchant,
        amount_usd: float,
        event_ts: float,
        *,
        location: City | None = None,
        entry_mode: str | None = None,
    ) -> dict:
        rng = self.rng
        if merchant.is_online:
            channel = "ECOM"
            entry_mode = "ECOM"
            where = location or user.home_city
            # Card-not-present location = geo-IP of the shopper's device.
            lat = where.lat + rng.gauss(0, 0.05)
            lon = where.lon + rng.gauss(0, 0.05)
            city, country, currency = where.name, where.country_code, user.home_city.currency
        else:
            where = CITIES[merchant.city_idx]  # type: ignore[index]
            channel = "ATM" if merchant.mcc == "6011" else "POS"
            if entry_mode is None:
                if not user.has_chip_card:
                    entry_mode = "SWIPE"
                elif channel == "ATM":
                    entry_mode = "CHIP"
                else:
                    r = rng.random()
                    entry_mode = "CONTACTLESS" if r < 0.55 else "CHIP" if r < 0.97 else "SWIPE"
            lat = merchant.lat + rng.gauss(0, 0.0005)  # type: ignore[operator]
            lon = merchant.lon + rng.gauss(0, 0.0005)  # type: ignore[operator]
            city, country, currency = where.name, where.country_code, where.currency

        return {
            "schema_version": SCHEMA_VERSION,
            "transaction_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "event_ts": _iso(event_ts),
            "user_id": user.user_id,
            "card_id": user.card_id,
            "merchant_id": merchant.merchant_id,
            "amount": _to_local(max(amount_usd, 0.5), currency),
            "currency": currency,
            "channel": channel,
            "entry_mode": entry_mode,
            "location_lat": round(lat, 5),
            "location_lon": round(lon, 5),
            "city": city,
            "country_code": country,
        }

    def _pick_category(self):
        return self.rng.choices(MERCHANT_CATEGORIES, cum_weights=self._category_cum, k=1)[0]

    def _pick_merchant(self, mcc: str, city_idx: int, prefer_online: bool | None = None) -> Merchant:
        cat = CATEGORY_BY_MCC[mcc]
        online = prefer_online if prefer_online is not None else self.rng.random() < cat.online_share
        pool = self.population.online_pool(mcc) if online else self.population.local_pool(city_idx, mcc)
        if pool is None:  # category has no merchants of the requested kind
            pool = self.population.local_pool(city_idx, mcc) or self.population.online_pool(mcc)
        assert pool is not None, f"no merchants for mcc={mcc}"
        return pool.pick(self.rng)

    def _typical_amount_usd(self, user: User, mcc: str) -> float:
        cat = CATEGORY_BY_MCC[mcc]
        return self.rng.lognormvariate(math.log(cat.median_amount_usd * user.spend_multiplier), cat.amount_sigma)

    def _normal_transaction(self, user: User, now: float) -> dict:
        cat = self._pick_category()
        merchant = self._pick_merchant(cat.mcc, user.home_city_idx)
        return self._transaction(user, merchant, self._typical_amount_usd(user, cat.mcc), now)

    def _big_ticket_transaction(self, user: User, now: float) -> dict:
        mcc = self.rng.choice(_BIG_TICKET_MCCS)
        merchant = self._pick_merchant(mcc, user.home_city_idx)
        amount = self._typical_amount_usd(user, mcc) * self.rng.uniform(6.0, 15.0)
        return self._transaction(user, merchant, amount, now)

    def _travel_transaction(self, user: User, now: float) -> dict:
        city_idx = self.rng.randrange(len(CITIES))
        mcc = self.rng.choice(("7011", "5812", "4121", "5411"))
        merchant = self._pick_merchant(mcc, city_idx, prefer_online=False)
        return self._transaction(user, merchant, self._typical_amount_usd(user, mcc), now)

    # ------------------------------------------------------------------ fraud scenarios

    def _far_city(self, home: City, min_km: float) -> City:
        candidates = [c for c in CITIES if haversine_km(home.lat, home.lon, c.lat, c.lon) >= min_km]
        return self.rng.choice(candidates or list(CITIES))

    def _launch_fraud_scenario(self, user: User, now: float) -> None:
        scenario = self.rng.choices(
            ("card_testing", "account_takeover", "cloned_card", "spending_spree"),
            weights=(0.30, 0.25, 0.25, 0.20),
            k=1,
        )[0]
        self.stats[f"scenario:{scenario}"] += 1
        events: list[tuple[float, dict, bool]] = getattr(self, f"_scenario_{scenario}")(user, now)
        for due, txn, is_fraud in events:
            kind = "fraud" if is_fraud else "normal"
            self._schedule(due, OutboundMessage(TRANSACTIONS, txn["user_id"], _encode(txn), kind))
            if is_fraud and self.rng.random() < self.config.chargeback_report_rate:
                self._schedule_chargeback(txn, due, friendly=False)

    def _scenario_card_testing(self, user: User, now: float) -> list[tuple[float, dict, bool]]:
        rng = self.rng
        origin = self._far_city(user.home_city, 2_000)
        events, t = [], now
        for _ in range(rng.randint(5, 12)):
            t += rng.uniform(2.0, 8.0)
            merchant = self._pick_merchant(rng.choice(_CARD_TESTING_MCCS), user.home_city_idx, prefer_online=True)
            events.append((t, self._transaction(user, merchant, rng.uniform(0.5, 4.99), t, location=origin), True))
        for _ in range(rng.randint(1, 2)):
            t += rng.uniform(10.0, 40.0)
            merchant = self._pick_merchant(rng.choice(_CASH_OUT_MCCS), user.home_city_idx, prefer_online=True)
            events.append((t, self._transaction(user, merchant, rng.uniform(400, 2_500), t, location=origin), True))
        return events

    def _scenario_account_takeover(self, user: User, now: float) -> list[tuple[float, dict, bool]]:
        rng = self.rng
        origin = self._far_city(user.home_city, 1_500)
        events, t = [], now
        for _ in range(rng.randint(1, 3)):
            t += rng.uniform(20.0, 90.0)
            merchant = self._pick_merchant(rng.choice(_CASH_OUT_MCCS), user.home_city_idx, prefer_online=True)
            amount = user.spend_multiplier * rng.uniform(600, 3_000)
            events.append((t, self._transaction(user, merchant, amount, t, location=origin), True))
        return events

    def _scenario_cloned_card(self, user: User, now: float) -> list[tuple[float, dict, bool]]:
        rng = self.rng
        # A genuine swipe at home establishes the cardholder's position...
        genuine = self._normal_transaction(user, now)
        events = [(now, genuine, False)]
        # ...then a counterfeit magstripe copy is used on another continent.
        far = self._far_city(user.home_city, 3_000)
        far_idx = CITIES.index(far)
        t = now + rng.uniform(120.0, 480.0)
        for _ in range(rng.randint(1, 3)):
            merchant = self._pick_merchant(rng.choice(_CLONE_MCCS), far_idx, prefer_online=False)
            events.append((t, self._transaction(user, merchant, rng.uniform(100, 900), t, entry_mode="SWIPE"), True))
            t += rng.uniform(60.0, 240.0)
        return events

    def _scenario_spending_spree(self, user: User, now: float) -> list[tuple[float, dict, bool]]:
        rng = self.rng
        events, t = [], now
        for _ in range(rng.randint(8, 14)):
            t += rng.uniform(8.0, 25.0)
            merchant = self._pick_merchant(rng.choice(_SPREE_MCCS), user.home_city_idx, prefer_online=False)
            txn = self._transaction(user, merchant, rng.uniform(20, 150), t, entry_mode="CONTACTLESS")
            events.append((t, txn, True))
        return events

    # ------------------------------------------------------------------ chargebacks & bad data

    def _schedule_chargeback(self, txn: dict, txn_time: float, friendly: bool) -> None:
        rng = self.rng
        if friendly:
            # Genuine purchases disputed by the cardholder; some falsely claim fraud.
            reason = rng.choices(("13.1", "13.3", "12.6", "10.4"), weights=(0.35, 0.2, 0.05, 0.4), k=1)[0]
        elif txn["channel"] == "ECOM":
            reason = "10.4"
        else:
            reason = "10.1" if txn["entry_mode"] == "SWIPE" else "10.3"
        reported = txn_time + rng.uniform(*self.config.chargeback_delay_s)
        chargeback = {
            "schema_version": SCHEMA_VERSION,
            "chargeback_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "transaction_id": txn["transaction_id"],
            "user_id": txn["user_id"],
            "reason_code": reason,
            "amount": txn["amount"],
            "currency": txn["currency"],
            "reported_ts": _iso(reported),
        }
        self._schedule(reported, OutboundMessage(CHARGEBACKS, txn["user_id"], _encode(chargeback), "chargeback"))

    def _emit_malformed(self, txn: dict, out: list[OutboundMessage]) -> None:
        bad = dict(txn)
        bad["transaction_id"] = str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
        defect = self.rng.choice(
            ("truncated", "negative_amount", "missing_merchant", "bad_currency", "bad_coordinates", "bad_timestamp")
        )
        if defect == "truncated":
            payload = _encode(bad)
            payload = payload[: len(payload) // 2]
        else:
            if defect == "negative_amount":
                bad["amount"] = -abs(bad["amount"])
            elif defect == "missing_merchant":
                bad.pop("merchant_id")
            elif defect == "bad_currency":
                bad["currency"] = "XXX"
            elif defect == "bad_coordinates":
                bad["location_lat"] = 123.456
            elif defect == "bad_timestamp":
                bad["event_ts"] = "not-a-timestamp"
            payload = _encode(bad)
        self.stats["malformed"] += 1
        out.append(OutboundMessage(TRANSACTIONS, txn["user_id"], payload, "malformed"))


def _encode(record: dict) -> bytes:
    return json.dumps(record, separators=(",", ":")).encode("utf-8")

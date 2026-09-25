"""Deterministic synthetic population of cardholders and merchants.

The same ``(n_users, n_merchants, seed)`` triple always yields the same entities,
which is what lets the dimension seeder and the event producer run as separate
processes and still agree on every ``user_id`` / ``merchant_id``.
"""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from itertools import accumulate

from common.reference import CARD_NETWORKS, CATEGORY_BY_MCC, CITIES, MERCHANT_CATEGORIES, City, MerchantCategory

SEGMENTS: tuple[tuple[str, float, float], ...] = (
    # (segment, share, spend multiplier)
    ("MASS_MARKET", 0.55, 1.0),
    ("AFFLUENT", 0.20, 1.8),
    ("STUDENT", 0.15, 0.6),
    ("SMALL_BUSINESS", 0.10, 1.5),
)

CARD_TIERS: tuple[tuple[str, float, int, int], ...] = (
    # (tier, share, min credit limit, max credit limit)
    ("STANDARD", 0.60, 2_000, 8_000),
    ("GOLD", 0.25, 8_000, 20_000),
    ("PLATINUM", 0.15, 20_000, 60_000),
)

NETWORK_WEIGHTS = (0.50, 0.30, 0.12, 0.08)

_NAME_PREFIXES = (
    "Summit",
    "Harbor",
    "Maple",
    "Blue Ridge",
    "Sunrise",
    "Urban",
    "Golden",
    "Silver Oak",
    "Northside",
    "Riverside",
    "Evergreen",
    "Liberty",
    "Pioneer",
    "Cedar",
    "Beacon",
    "Atlas",
    "Crescent",
    "Union",
    "Metro",
    "Lakeside",
    "Highland",
    "Keystone",
    "Orchard",
    "Pacific",
)

_NAME_NOUNS: dict[str, tuple[str, ...]] = {
    "5411": ("Market", "Grocers", "Fresh Foods", "Supermarket"),
    "5812": ("Bistro", "Kitchen", "Grill", "Trattoria"),
    "5814": ("Burgers", "Tacos", "Noodle Bar", "Coffee"),
    "5541": ("Fuel", "Gas & Go", "Petroleum", "Service Station"),
    "5912": ("Pharmacy", "Drugstore", "Apothecary", "Health"),
    "4899": ("Stream", "TV+", "Music", "Media"),
    "4121": ("Rides", "Cabs", "Mobility", "Car Service"),
    "5311": ("Department Store", "Emporium", "Outlet", "Goods Co"),
    "5691": ("Apparel", "Outfitters", "Boutique", "Denim Co"),
    "5815": ("Games", "Apps", "eBooks", "Digital"),
    "5999": ("Supply", "Gifts", "General Store", "Variety"),
    "6011": ("Bank ATM", "Credit Union ATM", "Cash Point", "ATM Network"),
    "4511": ("Airways", "Air", "Airlines", "Jet"),
    "7011": ("Hotel", "Inn", "Suites", "Resort"),
    "5732": ("Electronics", "Tech", "Computers", "Gadgets"),
    "5944": ("Jewelers", "Watches", "Diamonds", "Gold & Co"),
    "7995": ("Bet", "Casino", "Sportsbook", "Poker"),
    "6051": ("Crypto Exchange", "Coin", "Digital Assets", "Wallet"),
}


@dataclass(frozen=True, slots=True)
class User:
    user_id: str
    card_id: str
    home_city_idx: int
    card_network: str
    card_tier: str
    segment: str
    account_open_date: date
    credit_limit_usd: int
    activity_weight: float
    spend_multiplier: float
    has_chip_card: bool

    @property
    def home_city(self) -> City:
        return CITIES[self.home_city_idx]


@dataclass(frozen=True, slots=True)
class Merchant:
    merchant_id: str
    name: str
    mcc: str
    city_idx: int | None  # None => card-not-present (online) merchant
    lat: float | None
    lon: float | None
    popularity: float

    @property
    def is_online(self) -> bool:
        return self.city_idx is None

    @property
    def category(self) -> MerchantCategory:
        return CATEGORY_BY_MCC[self.mcc]


@dataclass
class _WeightedPool:
    """A list of items with pre-computed cumulative weights for fast sampling."""

    items: list = field(default_factory=list)
    cum_weights: list[float] = field(default_factory=list)

    def add(self, item, weight: float) -> None:
        self.items.append(item)
        self.cum_weights.append((self.cum_weights[-1] if self.cum_weights else 0.0) + weight)

    def pick(self, rng: random.Random):
        return rng.choices(self.items, cum_weights=self.cum_weights, k=1)[0]


@dataclass
class Population:
    users: list[User]
    merchants: list[Merchant]
    # (city_idx, mcc) -> pool of physical merchants
    local_merchants: dict[tuple[int, str], _WeightedPool]
    # mcc -> pool of online merchants
    online_merchants: dict[str, _WeightedPool]

    def local_pool(self, city_idx: int, mcc: str) -> _WeightedPool | None:
        return self.local_merchants.get((city_idx, mcc))

    def online_pool(self, mcc: str) -> _WeightedPool | None:
        return self.online_merchants.get(mcc)


def _generate_users(n_users: int, rng: random.Random, today: date) -> list[User]:
    city_idx = range(len(CITIES))
    city_cum = list(accumulate(c.population_weight for c in CITIES))
    segment_cum = list(accumulate(s[1] for s in SEGMENTS))
    tier_cum = list(accumulate(t[1] for t in CARD_TIERS))
    network_cum = list(accumulate(NETWORK_WEIGHTS))

    users: list[User] = []
    for i in range(n_users):
        segment, _, spend_mult = rng.choices(SEGMENTS, cum_weights=segment_cum, k=1)[0]
        tier_name, _, lo, hi = rng.choices(CARD_TIERS, cum_weights=tier_cum, k=1)[0]
        # ~3% of accounts are brand new, which matters for the new-account rule.
        age_days = rng.randint(0, 30) if rng.random() < 0.03 else rng.randint(31, 3650)
        users.append(
            User(
                user_id=f"U{i:07d}",
                card_id=f"tok_{rng.getrandbits(48):012x}",
                home_city_idx=rng.choices(city_idx, cum_weights=city_cum, k=1)[0],
                card_network=rng.choices(CARD_NETWORKS, cum_weights=network_cum, k=1)[0],
                card_tier=tier_name,
                segment=segment,
                account_open_date=today - timedelta(days=age_days),
                credit_limit_usd=int(rng.randint(lo, hi) // 500 * 500),
                activity_weight=rng.lognormvariate(0.0, 0.5),
                spend_multiplier=spend_mult * rng.lognormvariate(0.0, 0.25),
                has_chip_card=rng.random() < 0.98,
            )
        )
    return users


def _merchant_name(rng: random.Random, mcc: str, online: bool) -> str:
    prefix = rng.choice(_NAME_PREFIXES)
    noun = rng.choice(_NAME_NOUNS[mcc])
    return f"{prefix}{noun.replace(' ', '')}.com" if online else f"{prefix} {noun}"


def _generate_merchants(n_merchants: int, rng: random.Random) -> list[Merchant]:
    city_weights = [c.population_weight for c in CITIES]
    merchants: list[Merchant] = []

    def add(mcc: str, city_idx: int | None) -> None:
        lat = lon = None
        if city_idx is not None:
            city = CITIES[city_idx]
            # Scatter storefronts within a few kilometres of the city centre.
            lat = round(city.lat + rng.gauss(0, 0.04), 6)
            lon = round(city.lon + rng.gauss(0, 0.04), 6)
        merchants.append(
            Merchant(
                merchant_id=f"M{len(merchants):06d}",
                name=_merchant_name(rng, mcc, online=city_idx is None),
                mcc=mcc,
                city_idx=city_idx,
                lat=lat,
                lon=lon,
                popularity=rng.paretovariate(1.5),
            )
        )

    for cat in MERCHANT_CATEGORIES:
        n_cat = max(1, round(n_merchants * cat.traffic_share))
        n_online = max(2, round(n_cat * cat.online_share)) if cat.online_share > 0 else 0
        if cat.online_share < 1.0:
            # Guarantee every city has at least one storefront per physical category.
            n_physical = max(len(CITIES), n_cat - n_online)
            for city_idx in range(len(CITIES)):
                add(cat.mcc, city_idx)
            for city_idx in rng.choices(range(len(CITIES)), weights=city_weights, k=n_physical - len(CITIES)):
                add(cat.mcc, city_idx)
        for _ in range(n_online):
            add(cat.mcc, None)
    return merchants


def build_population(n_users: int, n_merchants: int, seed: int, today: date | None = None) -> Population:
    """Generate the full deterministic population for a given seed."""
    rng = random.Random(seed)
    today = today or date.today()
    users = _generate_users(n_users, rng, today)
    merchants = _generate_merchants(n_merchants, random.Random(seed + 1))

    local: dict[tuple[int, str], _WeightedPool] = defaultdict(_WeightedPool)
    online: dict[str, _WeightedPool] = defaultdict(_WeightedPool)
    for m in merchants:
        if m.city_idx is None:
            online[m.mcc].add(m, m.popularity)
        else:
            local[(m.city_idx, m.mcc)].add(m, m.popularity)
    return Population(users=users, merchants=merchants, local_merchants=dict(local), online_merchants=dict(online))

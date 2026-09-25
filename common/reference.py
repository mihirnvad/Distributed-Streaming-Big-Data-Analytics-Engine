"""Static reference data: cities, merchant categories, currencies, dispute codes.

This module is the single source of truth for reference data. The simulator uses
it to generate events, the seeder loads it into the warehouse dimensions, and the
streaming job uses the currency table to normalise amounts to USD.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    name: str
    country_code: str
    lat: float
    lon: float
    currency: str
    population_weight: float


@dataclass(frozen=True)
class MerchantCategory:
    mcc: str
    name: str
    risk_tier: str  # LOW | MEDIUM | HIGH
    traffic_share: float  # share of normal transaction volume
    median_amount_usd: float
    amount_sigma: float  # log-normal dispersion of ticket size
    online_share: float  # fraction of merchants in this category that are card-not-present


@dataclass(frozen=True)
class DisputeReason:
    code: str
    description: str
    is_fraud: bool


CITIES: tuple[City, ...] = (
    City("New York", "US", 40.7128, -74.0060, "USD", 10.0),
    City("Los Angeles", "US", 34.0522, -118.2437, "USD", 8.0),
    City("Chicago", "US", 41.8781, -87.6298, "USD", 6.0),
    City("Houston", "US", 29.7604, -95.3698, "USD", 5.0),
    City("Dallas", "US", 32.7767, -96.7970, "USD", 4.0),
    City("San Francisco", "US", 37.7749, -122.4194, "USD", 4.0),
    City("Phoenix", "US", 33.4484, -112.0740, "USD", 3.0),
    City("Philadelphia", "US", 39.9526, -75.1652, "USD", 3.0),
    City("Seattle", "US", 47.6062, -122.3321, "USD", 3.0),
    City("Miami", "US", 25.7617, -80.1918, "USD", 3.0),
    City("Boston", "US", 42.3601, -71.0589, "USD", 3.0),
    City("Atlanta", "US", 33.7490, -84.3880, "USD", 3.0),
    City("Denver", "US", 39.7392, -104.9903, "USD", 2.0),
    City("Austin", "US", 30.2672, -97.7431, "USD", 2.0),
    City("Toronto", "CA", 43.6532, -79.3832, "CAD", 3.0),
    City("Vancouver", "CA", 49.2827, -123.1207, "CAD", 2.0),
    City("Mexico City", "MX", 19.4326, -99.1332, "MXN", 2.0),
    City("Sao Paulo", "BR", -23.5505, -46.6333, "BRL", 2.0),
    City("London", "GB", 51.5074, -0.1278, "GBP", 4.0),
    City("Paris", "FR", 48.8566, 2.3522, "EUR", 2.0),
    City("Berlin", "DE", 52.5200, 13.4050, "EUR", 2.0),
    City("Madrid", "ES", 40.4168, -3.7038, "EUR", 1.0),
    City("Amsterdam", "NL", 52.3676, 4.9041, "EUR", 1.0),
    City("Dublin", "IE", 53.3498, -6.2603, "EUR", 1.0),
    City("Zurich", "CH", 47.3769, 8.5417, "CHF", 1.0),
    City("Bucharest", "RO", 44.4268, 26.1025, "RON", 0.5),
    City("Dubai", "AE", 25.2048, 55.2708, "AED", 1.0),
    City("Lagos", "NG", 6.5244, 3.3792, "NGN", 1.0),
    City("Johannesburg", "ZA", -26.2041, 28.0473, "ZAR", 1.0),
    City("Mumbai", "IN", 19.0760, 72.8777, "INR", 2.0),
    City("Bengaluru", "IN", 12.9716, 77.5946, "INR", 2.0),
    City("Singapore", "SG", 1.3521, 103.8198, "SGD", 1.0),
    City("Hong Kong", "HK", 22.3193, 114.1694, "HKD", 1.0),
    City("Tokyo", "JP", 35.6762, 139.6503, "JPY", 2.0),
    City("Seoul", "KR", 37.5665, 126.9780, "KRW", 1.0),
    City("Sydney", "AU", -33.8688, 151.2093, "AUD", 2.0),
)

MERCHANT_CATEGORIES: tuple[MerchantCategory, ...] = (
    MerchantCategory("5411", "Grocery Stores & Supermarkets", "LOW", 0.17, 48.0, 0.60, 0.05),
    MerchantCategory("5812", "Restaurants", "LOW", 0.12, 38.0, 0.55, 0.02),
    MerchantCategory("5814", "Fast Food", "LOW", 0.11, 12.0, 0.45, 0.10),
    MerchantCategory("5541", "Fuel Stations", "LOW", 0.08, 42.0, 0.40, 0.00),
    MerchantCategory("5912", "Pharmacies", "LOW", 0.05, 25.0, 0.60, 0.10),
    MerchantCategory("4899", "Streaming & Subscriptions", "LOW", 0.03, 15.0, 0.30, 1.00),
    MerchantCategory("4121", "Taxis & Rideshare", "MEDIUM", 0.06, 22.0, 0.50, 0.90),
    MerchantCategory("5311", "Department Stores", "MEDIUM", 0.05, 75.0, 0.70, 0.40),
    MerchantCategory("5691", "Clothing Stores", "MEDIUM", 0.05, 65.0, 0.60, 0.50),
    MerchantCategory("5815", "Digital Goods & Media", "MEDIUM", 0.06, 11.0, 0.60, 1.00),
    MerchantCategory("5999", "Miscellaneous Retail", "MEDIUM", 0.07, 40.0, 0.80, 0.50),
    MerchantCategory("6011", "ATM Cash Withdrawal", "MEDIUM", 0.04, 120.0, 0.60, 0.00),
    MerchantCategory("4511", "Airlines", "MEDIUM", 0.02, 420.0, 0.60, 0.90),
    MerchantCategory("7011", "Hotels & Lodging", "MEDIUM", 0.02, 260.0, 0.60, 0.70),
    MerchantCategory("5732", "Electronics Stores", "HIGH", 0.04, 180.0, 0.90, 0.60),
    MerchantCategory("5944", "Jewelry & Watches", "HIGH", 0.01, 350.0, 0.90, 0.30),
    MerchantCategory("7995", "Gambling & Betting", "HIGH", 0.01, 90.0, 1.00, 0.90),
    MerchantCategory("6051", "Crypto & Quasi-Cash", "HIGH", 0.01, 250.0, 1.00, 1.00),
)

CATEGORY_BY_MCC: dict[str, MerchantCategory] = {c.mcc: c for c in MERCHANT_CATEGORIES}

# Illustrative, static reference rates (USD per one unit of currency). A real
# deployment would source these from a daily FX feed into dim_currency.
USD_PER_UNIT: dict[str, float] = {
    "USD": 1.0,
    "CAD": 0.73,
    "MXN": 0.055,
    "BRL": 0.18,
    "GBP": 1.27,
    "EUR": 1.09,
    "CHF": 1.13,
    "RON": 0.22,
    "AED": 0.272,
    "NGN": 0.00065,
    "ZAR": 0.055,
    "INR": 0.012,
    "SGD": 0.75,
    "HKD": 0.128,
    "JPY": 0.0068,
    "KRW": 0.00073,
    "AUD": 0.66,
}

# Currencies with no minor unit are rounded to whole numbers.
ZERO_DECIMAL_CURRENCIES = frozenset({"JPY", "KRW"})

# Card-network style dispute reason codes (modelled on Visa's 10.x fraud and
# 12.x/13.x processing/consumer-dispute groups).
DISPUTE_REASONS: tuple[DisputeReason, ...] = (
    DisputeReason("10.1", "EMV Liability Shift Counterfeit Fraud", True),
    DisputeReason("10.3", "Other Fraud - Card-Present Environment", True),
    DisputeReason("10.4", "Other Fraud - Card-Absent Environment", True),
    DisputeReason("12.6", "Duplicate Processing", False),
    DisputeReason("13.1", "Merchandise/Services Not Received", False),
    DisputeReason("13.3", "Not as Described or Defective Merchandise", False),
)

DISPUTE_REASON_BY_CODE: dict[str, DisputeReason] = {r.code: r for r in DISPUTE_REASONS}

CHANNELS = ("POS", "ECOM", "ATM")
ENTRY_MODES = ("CHIP", "CONTACTLESS", "SWIPE", "ECOM", "MANUAL")
CARD_NETWORKS = ("VISA", "MASTERCARD", "AMEX", "DISCOVER")

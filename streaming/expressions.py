"""Reusable Spark column expressions (pure JVM - no Python UDFs)."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from common.geo import EARTH_RADIUS_KM
from common.reference import USD_PER_UNIT


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in km as a native Spark expression (matches common.geo)."""
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(d_lat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(d_lon / 2), 2)
    root = F.sqrt(a)
    # Clamp float error above 1.0. (Not F.least: it skips NULLs and would turn an unknown
    # coordinate into half the Earth's circumference instead of NULL.)
    clamped = F.when(root > 1.0, F.lit(1.0)).otherwise(root)
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(clamped)


def usd_rate(currency: Column) -> Column:
    """USD-per-unit rate for an ISO currency code; NULL for unknown currencies."""
    pairs = [part for code, rate in sorted(USD_PER_UNIT.items()) for part in (F.lit(code), F.lit(rate))]
    return F.try_element_at(F.create_map(*pairs), currency)


def to_usd(amount: Column, currency: Column) -> Column:
    return F.round(amount.cast("double") * usd_rate(currency), 2).cast("decimal(14,2)")


def date_key(ts: Column) -> Column:
    """YYYYMMDD integer surrogate key for dim_date."""
    return F.date_format(ts, "yyyyMMdd").cast("int")

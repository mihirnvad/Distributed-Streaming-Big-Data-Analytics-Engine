"""Geospatial helpers shared by the simulator and the streaming feature engine."""

from __future__ import annotations

import math

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two WGS-84 points in kilometres."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


def implied_speed_kmh(distance_km: float, elapsed_seconds: float, min_elapsed_seconds: float = 60.0) -> float:
    """Speed needed to cover ``distance_km`` in ``elapsed_seconds``.

    The elapsed time is floored (default one minute) so that two swipes a few
    milliseconds apart in the same shop do not produce an infinite speed.
    """
    hours = max(elapsed_seconds, min_elapsed_seconds) / 3600.0
    return distance_km / hours

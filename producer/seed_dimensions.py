"""Load reference data and the synthetic population into the warehouse dimensions.

Idempotent: reference dimensions are upserted, merchants are SCD Type 1
(overwritten in place), and cardholders go through the SCD Type 2 merge in
``dw.merge_dim_users`` so re-running with changed attributes versions them.

    python -m producer.seed_dimensions
    python -m producer.seed_dimensions --simulate-upgrades 500   # demo SCD2 history
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time

import psycopg

from common.reference import CITIES, DISPUTE_REASONS, MERCHANT_CATEGORIES, USD_PER_UNIT, ZERO_DECIMAL_CURRENCIES
from common.settings import load_settings
from producer.entities import CARD_TIERS, build_population

log = logging.getLogger("seed")


def wait_for_postgres(dsn: str, timeout_s: float = 120.0) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            with psycopg.connect(dsn, connect_timeout=5) as conn:
                conn.execute("SELECT 1 FROM dw.dim_date LIMIT 1")
            return
        except psycopg.Error as exc:
            if time.monotonic() > deadline:
                raise
            log.info("waiting for Postgres schema: %s", str(exc).strip().splitlines()[0])
            time.sleep(2)


def seed_reference(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dw.dim_location (city, country_code, latitude, longitude, currency_code)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (city, country_code) DO UPDATE
               SET latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
                   currency_code = EXCLUDED.currency_code
            """,
            [(c.name, c.country_code, c.lat, c.lon, c.currency) for c in CITIES],
        )
        cur.executemany(
            """
            INSERT INTO dw.dim_merchant_category (mcc, category_name, risk_tier) VALUES (%s, %s, %s)
            ON CONFLICT (mcc) DO UPDATE SET category_name = EXCLUDED.category_name, risk_tier = EXCLUDED.risk_tier
            """,
            [(c.mcc, c.name, c.risk_tier) for c in MERCHANT_CATEGORIES],
        )
        cur.executemany(
            """
            INSERT INTO dw.dim_currency (currency_code, usd_per_unit, minor_units) VALUES (%s, %s, %s)
            ON CONFLICT (currency_code) DO UPDATE SET usd_per_unit = EXCLUDED.usd_per_unit
            """,
            [(code, rate, 0 if code in ZERO_DECIMAL_CURRENCIES else 2) for code, rate in USD_PER_UNIT.items()],
        )
        cur.executemany(
            """
            INSERT INTO dw.dim_dispute_reason (reason_code, description, is_fraud) VALUES (%s, %s, %s)
            ON CONFLICT (reason_code) DO UPDATE SET description = EXCLUDED.description, is_fraud = EXCLUDED.is_fraud
            """,
            [(r.code, r.description, r.is_fraud) for r in DISPUTE_REASONS],
        )


def seed_merchants(conn: psycopg.Connection, merchants) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE _merchants (merchant_id text, merchant_name text, mcc char(4), city text, "
            "country_code char(2), latitude double precision, longitude double precision, is_online boolean) "
            "ON COMMIT DROP"
        )
        with cur.copy("COPY _merchants FROM STDIN") as copy:
            for m in merchants:
                city = None if m.city_idx is None else CITIES[m.city_idx]
                copy.write_row(
                    (
                        m.merchant_id,
                        m.name,
                        m.mcc,
                        city.name if city else None,
                        city.country_code if city else None,
                        m.lat,
                        m.lon,
                        m.is_online,
                    )
                )
        cur.execute(
            """
            INSERT INTO dw.dim_merchants (merchant_id, merchant_name, mcc, location_key, city, country_code,
                                          latitude, longitude, is_online)
            SELECT s.merchant_id, s.merchant_name, s.mcc, coalesce(l.location_key, -1), s.city, s.country_code,
                   s.latitude, s.longitude, s.is_online
            FROM _merchants s
            LEFT JOIN dw.dim_location l ON l.city = s.city AND l.country_code = s.country_code
            ON CONFLICT (merchant_id) DO UPDATE
               SET merchant_name = EXCLUDED.merchant_name, mcc = EXCLUDED.mcc,
                   location_key = EXCLUDED.location_key, city = EXCLUDED.city,
                   country_code = EXCLUDED.country_code, latitude = EXCLUDED.latitude,
                   longitude = EXCLUDED.longitude, is_online = EXCLUDED.is_online, updated_at = now()
             WHERE (dw.dim_merchants.merchant_name, dw.dim_merchants.mcc, dw.dim_merchants.city,
                    dw.dim_merchants.is_online)
                   IS DISTINCT FROM (EXCLUDED.merchant_name, EXCLUDED.mcc, EXCLUDED.city, EXCLUDED.is_online)
            """
        )
        return cur.rowcount


def seed_users(conn: psycopg.Connection, users, upgrades: set[str]) -> tuple[int, int]:
    tiers = [t[0] for t in CARD_TIERS]
    with conn.cursor() as cur:
        cur.execute("TRUNCATE staging.users")
        with cur.copy(
            "COPY staging.users (user_id, card_id, home_city, home_country_code, card_network, card_tier, "
            "segment, account_open_date, credit_limit_usd, has_chip_card) FROM STDIN"
        ) as copy:
            for u in users:
                tier, limit = u.card_tier, u.credit_limit_usd
                if u.user_id in upgrades and tier != tiers[-1]:
                    tier, limit = tiers[tiers.index(tier) + 1], int(limit * 1.5)
                city = CITIES[u.home_city_idx]
                copy.write_row(
                    (
                        u.user_id,
                        u.card_id,
                        city.name,
                        city.country_code,
                        u.card_network,
                        tier,
                        u.segment,
                        u.account_open_date,
                        limit,
                        u.has_chip_card,
                    )
                )
        inserted, expired = cur.execute("SELECT * FROM dw.merge_dim_users()").fetchone()
        cur.execute("TRUNCATE staging.users")
    return inserted, expired


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--simulate-upgrades",
        type=int,
        default=0,
        help="upgrade N random cardholders' card tier to demonstrate SCD Type 2 versioning",
    )
    args = parser.parse_args(argv)

    settings = load_settings()
    sim = settings.simulation
    wait_for_postgres(settings.postgres.dsn)

    started = time.perf_counter()
    population = build_population(sim.n_users, sim.n_merchants, sim.seed)
    upgrades: set[str] = set()
    if args.simulate_upgrades:
        upgrades = {u.user_id for u in random.Random().sample(population.users, args.simulate_upgrades)}

    with psycopg.connect(settings.postgres.dsn) as conn:
        seed_reference(conn)
        merchants_changed = seed_merchants(conn, population.merchants)
        inserted, expired = seed_users(conn, population.users, upgrades)
        conn.commit()

    log.info(
        "seeded %d cities, %d categories, %d currencies; merchants upserted=%d; "
        "users inserted=%d expired(SCD2)=%d in %.1fs",
        len(CITIES),
        len(MERCHANT_CATEGORIES),
        len(USD_PER_UNIT),
        merchants_changed,
        inserted,
        expired,
        time.perf_counter() - started,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

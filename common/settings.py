"""Environment-driven configuration shared by every service.

All knobs are read from environment variables (12-factor style) so the same
image can run locally, in docker-compose, or on a cluster with different values.
Defaults match docker-compose.yml.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from urllib.parse import quote_plus


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"))
    transactions_topic: str = field(default_factory=lambda: _env("KAFKA_TRANSACTIONS_TOPIC", "payments.transactions"))
    chargebacks_topic: str = field(default_factory=lambda: _env("KAFKA_CHARGEBACKS_TOPIC", "payments.chargebacks"))


@dataclass(frozen=True)
class PostgresSettings:
    host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: _env_int("POSTGRES_PORT", 5432))
    database: str = field(default_factory=lambda: _env("POSTGRES_DB", "fraud_dw"))
    user: str = field(default_factory=lambda: _env("POSTGRES_USER", "fraud"))
    password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "fraud"))

    @property
    def dsn(self) -> str:
        """libpq connection string for psycopg."""
        return (
            f"postgresql://{quote_plus(self.user)}:{quote_plus(self.password)}@{self.host}:{self.port}/{self.database}"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {"user": self.user, "password": self.password, "driver": "org.postgresql.Driver"}


@dataclass(frozen=True)
class LakehouseSettings:
    root: str = field(default_factory=lambda: _env("LAKEHOUSE_ROOT", "/data/lakehouse"))

    def _path(self, *parts: str) -> str:
        return "/".join([self.root.rstrip("/"), *parts])

    @property
    def bronze_transactions(self) -> str:
        return self._path("bronze", "transactions")

    @property
    def bronze_chargebacks(self) -> str:
        return self._path("bronze", "chargebacks")

    @property
    def silver_transactions(self) -> str:
        return self._path("silver", "transactions")

    @property
    def silver_chargebacks(self) -> str:
        return self._path("silver", "chargebacks")

    @property
    def silver_quarantine(self) -> str:
        return self._path("silver", "quarantine")

    @property
    def gold_transactions(self) -> str:
        return self._path("gold", "scored_transactions")

    def checkpoint(self, query_name: str) -> str:
        return self._path("_checkpoints", query_name)


@dataclass(frozen=True)
class StreamingSettings:
    trigger_interval: str = field(default_factory=lambda: _env("STREAM_TRIGGER_INTERVAL", "2 seconds"))
    slow_trigger_interval: str = field(default_factory=lambda: _env("STREAM_SLOW_TRIGGER_INTERVAL", "10 seconds"))
    watermark_delay: str = field(default_factory=lambda: _env("STREAM_WATERMARK_DELAY", "10 minutes"))
    window_duration: str = field(default_factory=lambda: _env("STREAM_WINDOW_DURATION", "5 minutes"))
    window_slide: str = field(default_factory=lambda: _env("STREAM_WINDOW_SLIDE", "1 minute"))
    max_offsets_per_trigger: int = field(default_factory=lambda: _env_int("STREAM_MAX_OFFSETS_PER_TRIGGER", 50_000))
    starting_offsets: str = field(default_factory=lambda: _env("STREAM_STARTING_OFFSETS", "earliest"))
    # Backpressure for Delta-to-Delta hops, so a backlog drains in bounded micro-batches.
    max_bytes_per_trigger: str = field(default_factory=lambda: _env("STREAM_MAX_BYTES_PER_TRIGGER", "16m"))
    user_state_idle_timeout: str = field(default_factory=lambda: _env("STREAM_USER_STATE_IDLE_TIMEOUT", "1 hour"))
    dimension_refresh_seconds: int = field(default_factory=lambda: _env_int("DIMENSION_REFRESH_SECONDS", 300))
    rules_path: str = field(default_factory=lambda: _env("FRAUD_RULES_PATH", "config/fraud_rules.yaml"))
    postgres_write_partitions: int = field(default_factory=lambda: _env_int("POSTGRES_WRITE_PARTITIONS", 4))


@dataclass(frozen=True)
class SimulationSettings:
    n_users: int = field(default_factory=lambda: _env_int("SIM_USERS", 100_000))
    n_merchants: int = field(default_factory=lambda: _env_int("SIM_MERCHANTS", 2_500))
    seed: int = field(default_factory=lambda: _env_int("SIM_SEED", 42))
    fraud_scenario_rate: float = field(default_factory=lambda: _env_float("SIM_FRAUD_SCENARIO_RATE", 0.0006))


@dataclass(frozen=True)
class Settings:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    postgres: PostgresSettings = field(default_factory=PostgresSettings)
    lakehouse: LakehouseSettings = field(default_factory=LakehouseSettings)
    streaming: StreamingSettings = field(default_factory=StreamingSettings)
    simulation: SimulationSettings = field(default_factory=SimulationSettings)


def load_settings() -> Settings:
    """Build a Settings snapshot from the current environment."""
    return Settings()

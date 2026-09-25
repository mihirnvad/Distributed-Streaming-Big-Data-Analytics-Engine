"""Shared fixtures.

Spark tests need a JVM (17 or 21). When one is unavailable they are skipped
locally, but CI sets ``REQUIRE_SPARK=1`` so a broken Spark setup fails the build
instead of silently skipping.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("fraud-pipeline-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.scheduler.mode", "FIFO")
            .config("spark.sql.streaming.stateStore.providerClass",
                    "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
            .config("spark.executorEnv.PYTHONPATH", str(ROOT))
            .getOrCreate()
        )  # fmt: skip
    except Exception as exc:  # pragma: no cover - environment dependent
        if os.environ.get("REQUIRE_SPARK") == "1":
            raise
        pytest.skip(f"Spark unavailable in this environment: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

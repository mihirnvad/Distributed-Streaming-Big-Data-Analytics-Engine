"""Entry point: start the medallion streaming queries in one Spark application.

    spark-submit streaming/run_pipeline.py --layers bronze,silver,gold

All queries share one driver and executor pool (FAIR-scheduled per layer), which
is far cheaper on a small cluster than one application per layer while keeping
independent checkpoints - any layer can be restarted or replayed on its own.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from pathlib import Path

from common.settings import PostgresSettings, load_settings
from streaming import bronze_ingestion, gold_sink, silver_transforms
from streaming.monitoring import PostgresProgressListener
from streaming.spark_session import build_spark

log = logging.getLogger("pipeline")

LAYERS = {"bronze": bronze_ingestion, "silver": silver_transforms, "gold": gold_sink}
# Touched once every requested query is running; the compose healthcheck watches it so
# the producer only starts when the pipeline can keep up (no startup backlog).
READY_FILE = Path(os.environ.get("PIPELINE_READY_FILE", "/tmp/pipeline-ready"))


def ensure_warehouse_partitions(pg: PostgresSettings, days_ahead: int = 30) -> None:
    import psycopg

    with psycopg.connect(pg.dsn, autocommit=True) as conn:
        created = conn.execute("SELECT dw.ensure_daily_partitions(current_date - 1, %s)", (days_ahead,)).fetchone()
        log.info("warehouse partitions ensured (%s created)", created[0] if created else 0)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)  # callback-server chatter on every foreachBatch
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--layers", default="bronze,silver,gold", help="comma-separated subset of: " + ",".join(LAYERS))
    parser.add_argument("--no-metrics", action="store_true", help="do not write progress to ops tables")
    args = parser.parse_args(argv)

    layers = [layer.strip() for layer in args.layers.split(",") if layer.strip()]
    unknown = set(layers) - set(LAYERS)
    if unknown:
        parser.error(f"unknown layer(s): {sorted(unknown)}")

    READY_FILE.unlink(missing_ok=True)  # a restarted container must not look ready early
    settings = load_settings()
    spark = build_spark("fraud-streaming-pipeline")
    if not args.no_metrics:
        spark.streams.addListener(PostgresProgressListener(settings.postgres.dsn))
    if "gold" in layers:
        ensure_warehouse_partitions(settings.postgres)

    queries = []
    for layer in ("bronze", "silver", "gold"):  # upstream first so tables exist for downstream readers
        if layer in layers:
            started = LAYERS[layer].start(spark, settings)
            queries.extend(started)
            log.info("layer %s started: %s", layer, ", ".join(q.name for q in started))

    READY_FILE.touch()
    stopping = threading.Event()

    def shutdown(signum, _frame) -> None:
        if stopping.is_set():
            return
        stopping.set()
        log.info("signal %s received: stopping %d queries gracefully", signum, len(queries))
        for q in queries:
            q.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        spark.streams.awaitAnyTermination()
    finally:
        READY_FILE.unlink(missing_ok=True)
        failed = [q for q in queries if q.exception() is not None]
        for q in queries:
            if q.isActive:
                q.stop()
        for q in failed:
            log.error("query %s failed: %s", q.name, q.exception())
    return 1 if failed and not stopping.is_set() else 0


if __name__ == "__main__":
    sys.exit(main())

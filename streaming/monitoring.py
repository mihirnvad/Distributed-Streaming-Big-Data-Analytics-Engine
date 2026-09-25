"""Streaming observability: persist every micro-batch's progress to Postgres.

A :class:`StreamingQueryListener` receives Spark's ``StreamingQueryProgress`` for
each micro-batch of each query. We extract latency, throughput, watermark,
state-store and Kafka-lag metrics and write them to ``ops.streaming_query_progress``
on a background thread (listener callbacks must never block the driver). The
dashboard reads this table to show p95 micro-batch latency and consumer lag.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener

log = logging.getLogger(__name__)

_INSERT_SQL = """
INSERT INTO ops.streaming_query_progress (
    run_id, batch_id, query_id, query_name, progress_ts, num_input_rows, input_rows_per_second,
    processed_rows_per_second, batch_duration_ms, trigger_execution_ms, add_batch_ms, get_batch_ms,
    query_planning_ms, wal_commit_ms, watermark, state_rows_total, state_memory_bytes,
    rows_dropped_by_watermark, max_offsets_behind_latest, progress_json
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, batch_id) DO NOTHING
"""


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if number != number else number  # NaN -> None


def _json_safe(value: Any) -> Any:
    """Replace NaN/Infinity (valid in Python's JSON, rejected by jsonb) with null."""
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return None
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def progress_to_row(progress: dict[str, Any]) -> tuple:
    """Flatten a StreamingQueryProgress JSON document into an ops table row."""
    durations = progress.get("durationMs") or {}
    state_ops = progress.get("stateOperators") or []
    sources = progress.get("sources") or []
    offsets_behind = [
        _num((s.get("metrics") or {}).get("maxOffsetsBehindLatest"))
        for s in sources
        if (s.get("metrics") or {}).get("maxOffsetsBehindLatest") is not None
    ]
    watermark = (progress.get("eventTime") or {}).get("watermark")
    if watermark and watermark.startswith("1970-01-01"):
        watermark = None  # Spark reports epoch 0 until the first event is seen
    return (
        progress["runId"],
        int(progress["batchId"]),
        progress["id"],
        progress.get("name") or progress["id"],
        progress["timestamp"],
        int(progress.get("numInputRows") or 0),
        _num(progress.get("inputRowsPerSecond")),
        _num(progress.get("processedRowsPerSecond")),
        progress.get("batchDuration"),
        durations.get("triggerExecution"),
        durations.get("addBatch"),
        durations.get("getBatch"),
        durations.get("queryPlanning"),
        durations.get("walCommit"),
        watermark,
        sum(int(s.get("numRowsTotal") or 0) for s in state_ops) if state_ops else None,
        sum(int(s.get("memoryUsedBytes") or 0) for s in state_ops) if state_ops else None,
        sum(int(s.get("numRowsDroppedByWatermark") or 0) for s in state_ops) if state_ops else None,
        int(max(offsets_behind)) if offsets_behind else None,
        json.dumps(_json_safe(progress)),
    )


class PostgresProgressListener(StreamingQueryListener):
    """Ships micro-batch progress to Postgres asynchronously; never raises into Spark."""

    def __init__(self, dsn: str, max_queue: int = 10_000) -> None:
        self._dsn = dsn
        self._queue: queue.Queue[tuple] = queue.Queue(maxsize=max_queue)
        self._thread = threading.Thread(target=self._drain, name="progress-writer", daemon=True)
        self._thread.start()

    # -- Spark callbacks -------------------------------------------------------

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark API
        log.info("query started: name=%s id=%s run=%s", event.name, event.id, event.runId)

    def onQueryProgress(self, event) -> None:  # noqa: N802
        try:
            progress = json.loads(event.progress.json)
            if int(progress.get("numInputRows") or 0) == 0 and not progress.get("stateOperators"):
                return  # idle trigger of a stateless query: nothing worth storing
            self._queue.put_nowait(progress_to_row(progress))
        except queue.Full:
            log.warning("progress queue full; dropping metrics for one batch")
        except Exception:  # pragma: no cover - defensive: metrics must never kill the job
            log.exception("failed to record streaming progress")

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        if event.exception:
            log.error("query %s terminated with error: %s", event.id, event.exception)
        else:
            log.info("query %s terminated", event.id)

    # -- background writer -----------------------------------------------------

    def _drain(self) -> None:
        import psycopg

        conn = None
        while True:
            row = self._queue.get()
            batch = [row]
            while len(batch) < 200:
                try:
                    batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            try:
                if conn is None or conn.closed:
                    conn = psycopg.connect(self._dsn, autocommit=True, application_name="spark-progress")
                with conn.cursor() as cur:
                    cur.executemany(_INSERT_SQL, batch)
            except Exception:
                log.exception("could not write %d progress rows; will reconnect", len(batch))
                if conn is not None:
                    conn.close()
                conn = None

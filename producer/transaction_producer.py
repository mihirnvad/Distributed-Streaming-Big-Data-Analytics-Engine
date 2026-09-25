"""High-throughput, multi-process Kafka producer for synthetic card payments.

Each worker process owns a disjoint shard of cardholders and runs its own
simulator plus librdkafka producer, so throughput scales with ``--workers``
while per-card ordering is preserved (messages are keyed by ``user_id``, which
pins every card to one Kafka partition).

Examples
--------
    python -m producer.transaction_producer --rate 2000
    python -m producer.transaction_producer --rate 12000 --workers 4 --duration 300
    python -m producer.transaction_producer --dry-run --rate 5 --duration 3
"""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import signal
import sys
import time
from dataclasses import asdict

from common.settings import load_settings
from producer.entities import build_population
from producer.simulator import CHARGEBACKS, TRANSACTIONS, SimulationConfig, TransactionSimulator

log = logging.getLogger("producer")

# Indices into the shared statistics array.
STAT_FIELDS = ("sent", "delivered", "errors", "fraud", "chargeback", "duplicate", "late", "malformed")
_IDX = {name: i for i, name in enumerate(STAT_FIELDS)}
TICK_SECONDS = 0.05


class _StdoutProducer:
    """Drop-in stand-in for confluent_kafka.Producer used by ``--dry-run``."""

    def __init__(self, max_print: int = 20) -> None:
        self.max_print = max_print
        self.printed = 0

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.printed < self.max_print:
            print(f"{topic}\t{key}\t{value.decode('utf-8', errors='replace')}", flush=True)
            self.printed += 1
        if on_delivery is not None:
            on_delivery(None, None)

    def poll(self, timeout: float = 0) -> int:
        return 0

    def flush(self, timeout: float = 0) -> int:
        return 0

    def __len__(self) -> int:
        return 0


def _build_kafka_producer(bootstrap: str, worker_idx: int):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": f"txn-producer-{worker_idx}",
            # Idempotent producer: no duplicates or reordering from librdkafka retries.
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.size": 1_000_000,
            "queue.buffering.max.messages": 500_000,
            "queue.buffering.max.kbytes": 512_000,
        }
    )


def _worker(
    worker_idx: int,
    n_workers: int,
    rate: float,
    sim_config: dict,
    stop: mp.synchronize.Event,
    shared_stats,
    dry_run: bool,
) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the parent coordinates shutdown
    settings = load_settings()
    sim_settings = settings.simulation
    population = build_population(sim_settings.n_users, sim_settings.n_merchants, sim_settings.seed)
    shard = population.users[worker_idx::n_workers]
    simulator = TransactionSimulator(
        population, shard, SimulationConfig(**sim_config), seed=sim_settings.seed * 1_000 + worker_idx
    )
    producer = _StdoutProducer() if dry_run else _build_kafka_producer(settings.kafka.bootstrap_servers, worker_idx)
    topics = {TRANSACTIONS: settings.kafka.transactions_topic, CHARGEBACKS: settings.kafka.chargebacks_topic}

    local = dict.fromkeys(STAT_FIELDS, 0)

    def on_delivery(err, _msg) -> None:
        if err is not None:
            local["errors"] += 1
        else:
            local["delivered"] += 1

    per_worker_rate = rate / n_workers
    carry = 0.0
    next_tick = time.monotonic()
    last_flush_stats = time.monotonic()

    while not stop.is_set():
        now_mono = time.monotonic()
        if now_mono < next_tick:
            time.sleep(next_tick - now_mono)
        elif now_mono - next_tick > 1.0:
            next_tick = now_mono  # fell far behind: don't try to burst-catch-up
        next_tick += TICK_SECONDS

        carry += per_worker_rate * TICK_SECONDS
        n_new = int(carry)
        carry -= n_new

        for msg in simulator.generate(n_new, time.time()):
            while True:
                try:
                    producer.produce(topics[msg.stream], key=msg.key, value=msg.value, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.05)  # local queue full: apply backpressure
            local["sent"] += 1
            if msg.kind in local:
                local[msg.kind] += 1
        producer.poll(0)

        if time.monotonic() - last_flush_stats >= 0.5:
            with shared_stats.get_lock():
                for name, value in local.items():
                    shared_stats[_IDX[name]] += value
            local = dict.fromkeys(STAT_FIELDS, 0)
            last_flush_stats = time.monotonic()

    remaining = producer.flush(30)
    with shared_stats.get_lock():
        for name, value in local.items():
            shared_stats[_IDX[name]] += value
    if remaining:
        log.warning("worker %d: %d messages not delivered before shutdown", worker_idx, remaining)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rate", type=float, default=2_000, help="target new transactions per second (total)")
    parser.add_argument("--workers", type=int, default=2, help="producer processes")
    parser.add_argument("--duration", type=float, default=0, help="seconds to run; 0 = until interrupted")
    parser.add_argument("--report-interval", type=float, default=5.0)
    parser.add_argument("--fraud-rate", type=float, default=None, help="override SIM_FRAUD_SCENARIO_RATE")
    parser.add_argument("--dry-run", action="store_true", help="print events to stdout instead of Kafka")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    settings = load_settings()
    fraud_rate = args.fraud_rate if args.fraud_rate is not None else settings.simulation.fraud_scenario_rate
    sim_config = asdict(SimulationConfig(fraud_scenario_rate=fraud_rate))
    workers = 1 if args.dry_run else max(1, args.workers)

    ctx = mp.get_context("spawn")
    stop = ctx.Event()
    shared_stats = ctx.Array("q", len(STAT_FIELDS))

    def request_stop(signum, _frame) -> None:
        log.info("received signal %s, shutting down", signum)
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    log.info(
        "starting %d worker(s) at %.0f tx/s -> %s (users=%d merchants=%d seed=%d fraud_rate=%.4f)",
        workers,
        args.rate,
        "stdout" if args.dry_run else settings.kafka.bootstrap_servers,
        settings.simulation.n_users,
        settings.simulation.n_merchants,
        settings.simulation.seed,
        fraud_rate,
    )
    procs = [
        ctx.Process(
            target=_worker,
            args=(i, workers, args.rate, sim_config, stop, shared_stats, args.dry_run),
            name=f"producer-{i}",
            daemon=False,
        )
        for i in range(workers)
    ]
    for p in procs:
        p.start()

    started = time.monotonic()
    last_report, last_sent = started, 0
    try:
        while not stop.is_set():
            stop.wait(min(args.report_interval, 0.5))
            now = time.monotonic()
            if args.duration and now - started >= args.duration:
                stop.set()
            if not any(p.is_alive() for p in procs):
                log.error("all producer workers exited")
                stop.set()
            if now - last_report >= args.report_interval or stop.is_set():
                snapshot = dict(zip(STAT_FIELDS, shared_stats[:], strict=True))
                rate = (snapshot["sent"] - last_sent) / max(now - last_report, 1e-9)
                if not args.dry_run:
                    log.info(
                        "%6.0fs | %7.0f msg/s | sent %s | delivered %s | fraud %s | chargebacks %s | "
                        "dup %s | late %s | malformed %s | errors %s",
                        now - started,
                        rate,
                        f"{snapshot['sent']:,}",
                        f"{snapshot['delivered']:,}",
                        f"{snapshot['fraud']:,}",
                        f"{snapshot['chargeback']:,}",
                        f"{snapshot['duplicate']:,}",
                        f"{snapshot['late']:,}",
                        f"{snapshot['malformed']:,}",
                        f"{snapshot['errors']:,}",
                    )
                last_report, last_sent = now, snapshot["sent"]
    finally:
        stop.set()
        for p in procs:
            p.join(timeout=60)
    errors = shared_stats[_IDX["errors"]]
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

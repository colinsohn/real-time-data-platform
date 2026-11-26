"""Reproducible performance benchmark and runnable CLI demonstration for real-time-data-platform."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import platform
import random
import time
from typing import Any, Dict, List, Sequence

from .pipeline import StreamingPipeline


def _percentile(sorted_vals: Sequence[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    rank = (pct / 100.0) * (len(sorted_vals) - 1)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    weight = rank - low
    return float(sorted_vals[low] * (1.0 - weight) + sorted_vals[high] * weight)


def generate_synthetic_workload(
    num_unique_events: int = 10000,
    duplicate_rate: float = 0.10,
    invalid_rate: float = 0.02,
    seed: int = 20261126,
) -> Dict[str, Any]:
    """Generate a seeded workload containing unique events, at-least-once duplicates, and invalid payloads."""
    rng = random.Random(seed)
    tenants = [f"tenant-{i:02d}" for i in range(1, 17)]
    event_types = [
        "payment_authorized",
        "payment_captured",
        "refund_issued",
        "checkout_started",
        "api_request",
    ]
    base_time = datetime(2026, 11, 26, 0, 0, 0, tzinfo=timezone.utc)

    unique_events: List[Dict[str, Any]] = []
    expected_total_amount_cents = 0

    for i in range(num_unique_events):
        tenant_id = rng.choice(tenants)
        ev_type = rng.choice(event_types)
        ts = base_time + timedelta(seconds=(i % 7200), milliseconds=rng.randint(0, 999))
        amount_cents = rng.randint(100, 25000) if ev_type != "api_request" else 0
        expected_total_amount_cents += amount_cents
        unique_events.append(
            {
                "schema_version": "1.0",
                "event_id": f"evt-{i:07d}",
                "tenant_id": tenant_id,
                "event_type": ev_type,
                "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "session_id": f"sess-{tenant_id}-{i % 250:04d}",
                "amount_cents": amount_cents,
                "latency_ms": round(rng.uniform(1.5, 180.0), 3),
                "attributes": {"region": rng.choice(["us-west1", "us-east1", "eu-west1"])},
            }
        )

    num_duplicates = int(round(num_unique_events * duplicate_rate))
    duplicates = [dict(rng.choice(unique_events)) for _ in range(num_duplicates)]

    num_invalid = int(round(num_unique_events * invalid_rate))
    invalids: List[Any] = []
    for j in range(num_invalid):
        mode = j % 3
        if mode == 0:
            invalids.append("{malformed_json_payload")
        elif mode == 1:
            invalids.append(
                {
                    "schema_version": "1.0",
                    "event_id": f"bad-evt-{j}",
                    "tenant_id": "tenant-01",
                    "event_type": "unknown_action",
                    "event_ts": "2026-11-26T01:00:00Z",
                    "session_id": "sess-bad",
                    "amount_cents": 500,
                }
            )
        else:
            invalids.append(
                {
                    "schema_version": "1.0",
                    "event_id": f"bad-amt-{j}",
                    "tenant_id": "tenant-02",
                    "event_type": "payment_captured",
                    "event_ts": "2026-11-26T01:00:00Z",
                    "session_id": "sess-bad",
                    "amount_cents": -100,
                }
            )

    stream: List[Any] = list(unique_events) + duplicates + invalids
    rng.shuffle(stream)

    return {
        "seed": seed,
        "num_unique_events": num_unique_events,
        "num_duplicates": num_duplicates,
        "num_invalid": num_invalid,
        "total_payloads": len(stream),
        "expected_total_amount_cents": expected_total_amount_cents,
        "stream": stream,
    }


def run_benchmark(
    num_unique_events: int = 10000,
    num_partitions: int = 8,
    duplicate_rate: float = 0.10,
    invalid_rate: float = 0.02,
    batch_size: int = 1000,
    seed: int = 20261126,
) -> Dict[str, Any]:
    """Run an end-to-end ingestion, Kafka partitioning, and SQL warehouse deduplication benchmark."""
    workload = generate_synthetic_workload(
        num_unique_events=num_unique_events,
        duplicate_rate=duplicate_rate,
        invalid_rate=invalid_rate,
        seed=seed,
    )
    pipeline = StreamingPipeline(num_partitions=num_partitions)

    t0 = time.perf_counter()
    ingest_summary = pipeline.ingest_raw_events(
        workload["stream"],
        ingested_at_ms=1795651200000,
    )
    t_ingest = time.perf_counter() - t0

    batch_latencies_ms: List[float] = []
    batches_processed = 0
    total_records_polled = 0
    total_duplicates_dropped = 0

    t_wh_start = time.perf_counter()
    while True:
        b_start = time.perf_counter()
        res = pipeline.process_next_micro_batch(max_records=batch_size, commit_offsets=True)
        b_elapsed_ms = (time.perf_counter() - b_start) * 1000.0
        if res.records_received == 0:
            break
        batch_latencies_ms.append(b_elapsed_ms)
        batches_processed += 1
        total_records_polled += res.records_received
        total_duplicates_dropped += res.duplicates_dropped
    t_warehouse = time.perf_counter() - t_wh_start
    t_total = t_ingest + t_warehouse

    sorted_lat = sorted(batch_latencies_ms)
    diag = pipeline.diagnostics()

    assert diag["warehouse_unique_events"] == workload["num_unique_events"], (
        f"Expected {workload['num_unique_events']} unique events, got {diag['warehouse_unique_events']}"
    )
    assert diag["warehouse_total_amount_cents"] == workload["expected_total_amount_cents"], (
        f"Financial conservation mismatch: {diag['warehouse_total_amount_cents']} != {workload['expected_total_amount_cents']}"
    )
    assert diag["dlq_total_records"] == workload["num_invalid"]
    assert diag["consumer_total_lag"] == 0

    return {
        "benchmark": "real-time-data-platform-day01",
        "environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python_version": platform.python_version(),
            "sql_engine": diag["sql_engine"],
        },
        "configuration": {
            "seed": seed,
            "num_partitions": num_partitions,
            "batch_size": batch_size,
            "num_unique_events": workload["num_unique_events"],
            "num_duplicates": workload["num_duplicates"],
            "num_invalid": workload["num_invalid"],
            "total_submitted_payloads": workload["total_payloads"],
        },
        "throughput": {
            "ingest_elapsed_sec": round(t_ingest, 4),
            "ingest_payloads_per_sec": round(workload["total_payloads"] / max(t_ingest, 1e-9), 2),
            "warehouse_elapsed_sec": round(t_warehouse, 4),
            "warehouse_records_per_sec": round(total_records_polled / max(t_warehouse, 1e-9), 2),
            "end_to_end_elapsed_sec": round(t_total, 4),
            "end_to_end_payloads_per_sec": round(workload["total_payloads"] / max(t_total, 1e-9), 2),
        },
        "micro_batch_latency_ms": {
            "batches": batches_processed,
            "p50_ms": round(_percentile(sorted_lat, 50.0), 3),
            "p95_ms": round(_percentile(sorted_lat, 95.0), 3),
            "p99_ms": round(_percentile(sorted_lat, 99.0), 3),
            "max_ms": round(max(sorted_lat) if sorted_lat else 0.0, 3),
            "mean_ms": round(sum(sorted_lat) / max(len(sorted_lat), 1), 3),
        },
        "verification": {
            "valid_produced": ingest_summary.valid_produced,
            "dead_letter_produced": ingest_summary.dead_letter_produced,
            "warehouse_unique_events": diag["warehouse_unique_events"],
            "duplicates_dropped": total_duplicates_dropped,
            "expected_total_amount_cents": workload["expected_total_amount_cents"],
            "observed_total_amount_cents": diag["warehouse_total_amount_cents"],
            "amount_conservation_verified": True,
            "consumer_total_lag": diag["consumer_total_lag"],
            "partition_watermarks": diag["raw_topic_watermarks"],
        },
    }


def run_demo() -> int:
    """Run an interactive demonstration showing partitioning, DLQ quarantine, crash recovery, and BigQuery SQL."""
    pipeline = StreamingPipeline(num_partitions=4)
    sample_payloads: List[Any] = [
        {
            "schema_version": "1.0",
            "event_id": "evt-demo-001",
            "tenant_id": "acme-pay",
            "event_type": "payment_captured",
            "event_ts": "2026-11-26T00:10:00Z",
            "session_id": "sess-acme-1",
            "amount_cents": 4500,
            "latency_ms": 18.4,
            "attributes": {"currency": "USD"},
        },
        {
            "schema_version": "1.0",
            "event_id": "evt-demo-002",
            "tenant_id": "acme-pay",
            "event_type": "payment_captured",
            "event_ts": "2026-11-26T00:25:00Z",
            "session_id": "sess-acme-2",
            "amount_cents": 5500,
            "latency_ms": 21.6,
            "attributes": {"currency": "USD"},
        },
        # Duplicate retry of evt-demo-001
        {
            "schema_version": "1.0",
            "event_id": "evt-demo-001",
            "tenant_id": "acme-pay",
            "event_type": "payment_captured",
            "event_ts": "2026-11-26T00:10:00Z",
            "session_id": "sess-acme-1",
            "amount_cents": 4500,
            "latency_ms": 18.4,
            "attributes": {"currency": "USD"},
        },
        {
            "schema_version": "1.0",
            "event_id": "evt-demo-003",
            "tenant_id": "globex-cloud",
            "event_type": "payment_authorized",
            "event_ts": "2026-11-26T00:30:00Z",
            "session_id": "sess-globex-1",
            "amount_cents": 12000,
            "latency_ms": 11.2,
            "attributes": {"currency": "USD"},
        },
        # Invalid poison-pill payload -> routed to events.dlq.v1
        "{corrupted_json: true",
    ]

    summary = pipeline.ingest_raw_events(sample_payloads, ingested_at_ms=1795651200000)
    print("=== 1. Ingestion & Schema Validation ===")
    print(json.dumps(summary.__dict__, indent=2))

    # Simulate worker crash before offset commit (commit_offsets=False)
    crash_attempt = pipeline.process_next_micro_batch(max_records=10, commit_offsets=False)
    print("\n=== 2. First Micro-Batch (Simulated Crash Before Kafka Offset Commit) ===")
    print(json.dumps(crash_attempt.__dict__, indent=2))

    # Worker restarts and re-polls from uncommitted offset 0
    recovery_attempt = pipeline.process_next_micro_batch(max_records=10, commit_offsets=True)
    print("\n=== 3. Recovery Micro-Batch (Re-Polled Uncommitted Offsets -> 0 Duplicate Rows Added) ===")
    print(json.dumps(recovery_attempt.__dict__, indent=2))

    print("\n=== 4. SQL Warehouse Hourly Mart (mart_tenant_hourly_metrics) ===")
    print(json.dumps(pipeline.warehouse.fetch_hourly_mart(), indent=2))

    print("\n=== 5. Compiled BigQuery Idempotent MERGE Statement ===")
    print(pipeline.bq_compiler.compile_idempotent_merge().strip())
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rt-data-platform")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("demo", help="Run the interactive streaming & recovery demonstration")

    bench_p = sub.add_parser("benchmark", help="Run reproducible throughput and latency benchmark")
    bench_p.add_argument("--events", type=int, default=10000)
    bench_p.add_argument("--partitions", type=int, default=8)
    bench_p.add_argument("--duplicate-rate", type=float, default=0.10)
    bench_p.add_argument("--invalid-rate", type=float, default=0.02)
    bench_p.add_argument("--batch-size", type=int, default=1000)
    bench_p.add_argument("--seed", type=int, default=20261126)
    bench_p.add_argument("--output", type=Path, default=None)

    args = parser.parse_args(argv)
    if args.cmd == "demo":
        return run_demo()
    if args.cmd == "benchmark":
        report = run_benchmark(
            num_unique_events=args.events,
            num_partitions=args.partitions,
            duplicate_rate=args.duplicate_rate,
            invalid_rate=args.invalid_rate,
            batch_size=args.batch_size,
            seed=args.seed,
        )
        formatted = json.dumps(report, indent=2)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(formatted + "\n", encoding="utf-8")
        print(formatted)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

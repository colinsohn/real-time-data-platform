"""Reproducible throughput and latency benchmark for the real-time streaming data platform."""
import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import platform
import time

import duckdb

from platform_core.processor import StatefulStreamProcessor
from platform_core.warehouse import WarehouseSink


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(math.ceil(p * len(sorted_vals))) - 1))
    return round(sorted_vals[idx], 4)


def run_benchmark(num_events: int = 50_000, batch_size: int = 1_000) -> dict:
    base_ms = 1_764_115_200_000
    accounts = [f"acct-{i:04d}" for i in range(128)]

    records = []
    for i in range(num_events):
        # 5% duplicate retries, 1% late arrivals, 1% invalid payloads
        if i > 0 and i % 20 == 0:
            records.append(records[i - 1])
            continue
        if i > 0 and i % 100 == 0:
            records.append({
                "event_id": f"evt-invalid-{i}",
                "event_type": "invalid_type",
                "account_id": accounts[i % len(accounts)],
                "occurred_at_ms": base_ms + i * 25,
                "ingested_at_ms": base_ms + i * 25 + 10,
                "amount_cents": 1000,
                "currency": "USD",
                "schema_version": 1,
                "payload": {},
            })
            continue

        jitter_ms = -120_000 if (i > 5_000 and i % 100 == 50) else ((i % 7) - 3) * 500
        ev_type = "refund_issued" if i % 15 == 0 else ("payment_authorized" if i % 5 == 0 else "order_created")
        records.append({
            "event_id": f"evt-{i:07d}",
            "event_type": ev_type,
            "account_id": accounts[i % len(accounts)],
            "occurred_at_ms": base_ms + i * 25 + jitter_ms,
            "ingested_at_ms": base_ms + i * 25 + 20,
            "amount_cents": 500 + (i % 200) * 25,
            "currency": "USD",
            "schema_version": 1,
            "payload": {"seq": i, "channel": "web"},
        })

    processor = StatefulStreamProcessor(
        window_size_ms=60_000,
        max_out_of_orderness_ms=10_000,
        num_partitions=16,
        dedup_capacity=100_000,
    )

    batch_latencies_ms = []
    t0 = time.perf_counter()
    for start in range(0, len(records), batch_size):
        b0 = time.perf_counter()
        for rec in records[start : start + batch_size]:
            processor.ingest(rec, observed_at_ms=base_ms)
        batch_latencies_ms.append((time.perf_counter() - b0) * 1000.0)
    processor.flush_all()
    stream_wall_s = time.perf_counter() - t0

    s0 = time.perf_counter()
    sink = WarehouseSink(":memory:")
    sink.sink_events(processor.accepted_events)
    sink.sink_rollups(processor.closed_windows)
    sink.sink_dlq(processor.dlq)
    reconciliation = sink.verify_reconciliation(processor.stats())
    sink_wall_s = time.perf_counter() - s0
    sink.close()

    batch_latencies_ms.sort()
    total_wall_s = stream_wall_s + sink_wall_s

    return {
        "format_version": 1,
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "duckdb_version": duckdb.__version__,
        "platform": f"{platform.system()}-{platform.machine()}",
        "workload": {
            "total_input_events": num_events,
            "batch_size": batch_size,
            "distinct_accounts": len(accounts),
            "partitions": 16,
            "window_size_ms": 60_000,
            "max_out_of_orderness_ms": 10_000,
        },
        "throughput": {
            "stream_processing_wall_milliseconds": round(stream_wall_s * 1000.0, 3),
            "warehouse_sink_wall_milliseconds": round(sink_wall_s * 1000.0, 3),
            "total_wall_milliseconds": round(total_wall_s * 1000.0, 3),
            "stream_events_per_second": round(num_events / stream_wall_s, 2),
            "end_to_end_events_per_second": round(num_events / total_wall_s, 2),
        },
        "batch_latency_ms": {
            "batches": len(batch_latencies_ms),
            "min_ms": round(batch_latencies_ms[0], 4),
            "p50_ms": percentile(batch_latencies_ms, 0.50),
            "p95_ms": percentile(batch_latencies_ms, 0.95),
            "p99_ms": percentile(batch_latencies_ms, 0.99),
            "max_ms": round(batch_latencies_ms[-1], 4),
        },
        "processor_stats": processor.stats(),
        "warehouse_reconciliation": reconciliation,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=50_000)
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument("--report", type=Path, default=None)
    args = parser.parse_args()
    report = run_benchmark(num_events=args.events, batch_size=args.batch_size)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

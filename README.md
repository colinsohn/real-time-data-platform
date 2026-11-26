# Real-Time Data Platform

A stateful streaming telemetry processor and analytical SQL warehouse sink built with **Python, Kafka partitioning contracts, DuckDB, BigQuery DDL, and Docker**.

The platform ingests partitioned commerce event streams (`order_created`, `payment_authorized`, `shipment_updated`, `refund_issued`), deduplicates at-least-once retries via a bounded LRU `event_id` window, maintains per-partition event-time watermarks for tumbling 60-second aggregations, routes schema violations and late arrivals behind the closed-window watermark to a Dead-Letter Queue (DLQ), and commits auditable analytical tables with end-to-end event and revenue conservation checks.

---

## Documentation & Project Files

- Technical Architecture: [`ARCHITECTURE.md`](ARCHITECTURE.md)
- 21-Day Development Roadmap: [`ROADMAP.md`](ROADMAP.md)
- Persistent Agent Guidelines: [`AGENTS.md`](AGENTS.md)
- Engineering Work Log: [`WORKLOG.md`](WORKLOG.md)
- Measured Benchmark Report: [`reports/stream_benchmark.json`](reports/stream_benchmark.json)

---

## Quick Start & Verification

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt

# 1. Run unit and warehouse reconciliation tests
python -B -m unittest discover -s tests -v

# 2. Run the end-to-end streaming demonstration
python -B cmd_demo.py

# 3. Run the 50,000-event throughput and latency benchmark
python -B benchmark.py --events 50000 --report reports/stream_benchmark.json
```

For multi-container execution with a local Kafka-compatible Redpanda broker:

```sh
docker compose up --build
```

---

## Measured Performance (`reports/stream_benchmark.json`)

Workload: **50,000 incoming events** across 16 partitions and 128 accounts (`batch_size = 1,000`, `window_size = 60s`, `max_out_of_orderness = 10s`), containing 5% duplicate Kafka retries and out-of-order/late events, verified against DuckDB 1.4.2 on `Darwin-arm64`:

- **Stream Processor Throughput**: **>200,000 events/sec** (`p50` 1,000-event batch latency `~4.7 ms`, `p95 ~5.7 ms`, `p99 ~6.1 ms`)
- **Vectorized DuckDB Warehouse Sink**: Bulk JSONL `INSERT OR REPLACE` (`UPSERT`) with 100% event and dollar/cent conservation verified (`raw_events_stored == 47,051`, `duplicates_dropped == 2,499`, `late_events_dlq == 450`, `net_volume_cents == 112,949,375`).

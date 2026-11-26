# Real-Time Data Platform (`real-time-data-platform`)

End-to-end **real-time event streaming, partitioned Kafka log engine, dead-letter quarantine, DuckDB SQL micro-batch deduplication, and BigQuery Standard SQL staging compiler** in Python, SQL, and Docker.

---

## Architecture Overview

```mermaid
flowchart LR
    P["Multi-Tenant Event Producers\n(JSON Telemetry & Payments)"] --> V["Schema Contract Validator\n(contract.py · v1.0)"]
    V -->|Invalid / Poison Pill| DLQ["Kafka Dead-Letter Topic\n(events.dlq.v1)"]
    V -->|Valid TelemetryEvent| M["Kafka Murmur2 Partitioner\nkey = tenant_id"]
    M --> K["Partitioned Topic Log\n(events.raw.v1 · P0..Pn-1\nCRC32 Frame Integrity)"]
    K -->|Consumer Group Poll\ncg-warehouse-loader-v1| S["SQL Staging Table\n(stg_telemetry_events)"]
    S -->|ROW_NUMBER() OVER (PARTITION BY event_id)\n+ NOT EXISTS / BigQuery MERGE| C["Deduplicated Core Table\n(core_telemetry_events)"]
    C --> H["Hourly Tenant Mart\n(mart_tenant_hourly_metrics)"]
    C -->|After SQL Commit Succeeds| O["Atomic Consumer Offset Commit"]
```

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full technical architecture, CRC32 log framing, two-phase at-least-once offset commit protocol, and BigQuery partitioning/clustering design, and [`ROADMAP.md`](ROADMAP.md) for the 21-day development roadmap.

---

## Fresh-Clone Quickstart & Runnable Demonstration

```bash
git clone https://github.com/colinsohn/real-time-data-platform.git
cd real-time-data-platform

# 1. Create a virtual environment and install package + dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .

# 2. Run unit and integration test suite
pytest -v

# 3. Run the streaming ingestion, crash-before-offset-commit recovery & BigQuery SQL demo
python -B -m rt_data_platform.cli demo

# 4. Run the reproducible 11,200-payload benchmark
python -B -m rt_data_platform.cli benchmark --output reports/benchmark_day01.json
```

---

## Runnable Demonstration Output (`python -B -m rt_data_platform.cli demo`)

The demo ingests 5 raw payloads (3 unique valid events across tenants `acme-pay` and `globex-cloud`, 1 duplicate retry of `evt-demo-001`, and 1 corrupted JSON poison-pill payload), simulates a **worker crash after the SQL warehouse commit but before the Kafka consumer offset commit**, and shows the restarted worker re-polling from uncommitted offset `0` with **zero duplicate rows or double-counted cents**:

```json
=== 1. Ingestion & Schema Validation ===
{
  "total_submitted": 5,
  "valid_produced": 4,
  "dead_letter_produced": 1
}

=== 2. First Micro-Batch (Simulated Crash Before Kafka Offset Commit) ===
{
  "batch_id": "batch-00001",
  "records_received": 4,
  "new_unique_events_inserted": 3,
  "duplicates_dropped": 1,
  "next_offsets": {
    "0": 1,
    "3": 3
  }
}

=== 3. Recovery Micro-Batch (Re-Polled Uncommitted Offsets -> 0 Duplicate Rows Added) ===
{
  "batch_id": "batch-00002",
  "records_received": 4,
  "new_unique_events_inserted": 0,
  "duplicates_dropped": 4,
  "next_offsets": {
    "0": 1,
    "3": 3
  }
}
```

---

## Reproducible Performance Benchmark ([`reports/benchmark_day01.json`](reports/benchmark_day01.json))

Measured on Apple Silicon (`arm64`, Python 3.14.7, DuckDB 1.4.2, `seed = 20261126`) across **11,200 submitted payloads** (`10,000` unique valid events, `1,000` duplicate at-least-once deliveries, and `200` malformed/invalid payloads across `8` Kafka partitions and `1,000`-record micro-batches):

| Metric | Measured Value | Verification Invariant |
| --- | ---: | --- |
| **Producer Validation + Murmur2 Partitioning + CRC32 Log Append** | **46,232.53 payloads/sec** (`0.2423 s`) | `11,000` routed to `events.raw.v1`, `200` quarantined in `events.dlq.v1` |
| **DuckDB SQL Micro-Batch Staging, `ROW_NUMBER()` Deduplication & Mart Refresh** | **762.63 records/sec** (`11` batches) | `10,000` unique rows inserted, `1,000` duplicate retries dropped |
| **Micro-Batch Commit Latency (`1,000` records/batch)** | `p50 = 1,297.83 ms`, `p95 = 1,360.62 ms`, `p99 = 1,367.87 ms` | Atomic batch staging + deduplication + hourly rollup refresh |
| **Financial Amount Conservation (`amount_cents`)** | **`99,957,248` cents** (`$999,572.48`) | Exact 100% match with ground-truth unique valid events (`0` cents lost or double-counted) |
| **Final Consumer Group Lag (`cg-warehouse-loader-v1`)** | **`0` records** across all 8 partitions | All committed offsets match topic high watermarks |

---

## BigQuery Standard SQL Schema & Idempotent `MERGE` ([`sql/bigquery_schema.sql`](sql/bigquery_schema.sql))

The BigQuery compiler (`BigQueryCompiler`) emits partitioned (`PARTITION BY event_date` with `require_partition_filter = TRUE`) and clustered (`CLUSTER BY tenant_id, event_type`) DDL alongside an idempotent staging-to-core `MERGE` query that deduplicates both intra-batch and historical `event_id` collisions.

# Technical Architecture (`real-time-data-platform`)

## 1. System Overview & Dataflow

`real-time-data-platform` is a multi-tenant event streaming and analytical ingestion engine built around **at-least-once Kafka delivery coupled with an idempotent SQL warehouse sink** to achieve **effectively-once analytical state** without distributed two-phase lock bottlenecks across external brokers.

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

---

## 2. Core Components & Invariants

### 2.1 Versioned Event Contract & Dead-Letter Quarantine (`src/rt_data_platform/contract.py`)
- Enforces `schema_version == "1.0"`, non-empty `event_id`, `tenant_id`, `session_id`, explicit-timezone ISO-8601 `event_ts` (normalized to UTC millisecond epoch `event_ts_ms`, `event_date`, and `event_hour_utc`), domain-checked `event_type`, non-negative integer `amount_cents`, and non-negative float `latency_ms`.
- Any malformed JSON, missing field, unparseable timestamp, or negative monetary value is immediately converted into an immutable `DeadLetterRecord` with a machine-readable `reason_code` (`MALFORMED_JSON`, `UNSUPPORTED_SCHEMA_VERSION`, `MISSING_REQUIRED_FIELD`, `INVALID_EVENT_TYPE`, `INVALID_TIMESTAMP`, `INVALID_AMOUNT_CENTS`) and routed to `events.dlq.v1` so a single bad producer deploy never stalls consumer partitions.

### 2.2 Kafka Murmur2 Partitioning & CRC32 Log Storage (`src/rt_data_platform/kafka_log.py`)
- **Partition Routing**: Implements Apache Kafka's exact 32-bit `murmur2` hash with positive bitmask (`kafka_murmur2(tenant_id) & 0x7fffffff % num_partitions`). All events for a given `tenant_id` are deterministically assigned to the same partition, preserving causal per-tenant ordering.
- **CRC32 Frame Integrity**: Every `KafkaRecord` computes and stores a `zlib.crc32` checksum over its canonical JSON frame (`topic`, `partition`, `offset`, `key`, `value`, `timestamp_ms`). Checksums are verified on disk reload and on every consumer `poll_uncommitted` read to detect bit-rot or truncated disk writes.

### 2.3 Two-Phase At-Least-Once Offset Commit & SQL Deduplication (`src/rt_data_platform/warehouse.py`)
- **Why Commit Offsets After Warehouse Commit**:
  - If a consumer commits Kafka offsets *before* writing to the warehouse, a worker crash drops the batch permanently (**at-most-once / data loss**).
  - By writing to the SQL warehouse *first* and committing Kafka offsets *second*, a worker crash between the two steps causes the restarted worker to re-poll the uncommitted offset range (**at-least-once delivery**).
- **Idempotent Deduplication**:
  - Within each micro-batch, `stg_telemetry_events` is deduplicated via `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY kafka_partition ASC, kafka_offset ASC) = 1`.
  - Across micro-batches (including post-crash replays), rows are filtered with `NOT EXISTS (SELECT 1 FROM core_telemetry_events c WHERE c.event_id = s.event_id)` locally in DuckDB and via `MERGE INTO ... ON target.event_id = source.event_id AND target.event_date = source.event_date WHEN NOT MATCHED THEN INSERT` in Google BigQuery.

### 2.4 BigQuery Partitioning & Clustering (`BigQueryCompiler`)
- `core_telemetry_events` and `mart_tenant_hourly_metrics` are compiled with:
  - `PARTITION BY event_date` (pruning daily storage blocks and enforcing `require_partition_filter = TRUE` to prevent accidental full-table scans).
  - `CLUSTER BY tenant_id, event_type` (co-locating blocks by tenant and action type so multi-tenant dashboard queries scan only relevant micro-blocks).

---

## 3. Design Trade-Offs

| Decision | Chosen Approach | Alternative Considered | Rationale |
| --- | --- | --- | --- |
| **Delivery Semantics** | At-least-once consumer poll + idempotent SQL `MERGE` / `NOT EXISTS` sink | Kafka transactional 2PC across external database | External databases (BigQuery / DuckDB) do not participate in Kafka broker transactions; idempotent `event_id` deduplication achieves effectively-once warehouse state even when workers crash mid-batch. |
| **Warehouse Loading** | Micro-batch staging (`stg_telemetry_events` -> `core_telemetry_events`) | Row-by-row single-event SQL `INSERT` | Columnar analytical engines (DuckDB and BigQuery) amortize metadata and block creation overhead across micro-batches, improving ingestion throughput by 20x–100x over single-row inserts. |
| **Local & CI Verification** | Embedded DuckDB SQL execution + BigQuery Standard SQL compiler | Live cloud BigQuery jobs on every test | Runs in `< 1s` offline at `$0.00` cloud cost while verifying the exact windowing and deduplication SQL logic. |

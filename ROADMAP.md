# 21-Day Development Roadmap — Real-Time Data Platform

| Day | Milestone | Status |
| --- | --- | --- |
| **Day 1** | Canonical Kafka event schema, deterministic partition router, BigQuery partitioned/clustered DDL generator, and unit tests. | **Completed (Nov 26, 2026)** |
| **Day 2** | Stateful stream processor (bounded LRU `event_id` deduplication, per-partition event-time watermarks, tumbling windows, DLQ) + DuckDB warehouse sink, reconciliation verifier, benchmark, and GitHub Actions CI. | **Completed (Nov 26, 2026)** |
| **Day 3** | Persistent consumer offset checkpointing and crash/replay recovery tests verifying zero double-counting across restarts. | Pending |
| **Day 4** | Sliding and session window aggregators with configurable gap timeouts and memory-bounded state eviction. | Pending |
| **Day 5** | Schema evolution and compatibility registry (v1 -> v2 backward/forward field migration and schema-drift rejection). | Pending |
| **Day 6** | Dead-Letter Queue (DLQ) replay CLI with quarantine inspection, payload repair rules, and audit trail logging. | Pending |
| **Day 7** | Partition rebalance coordinator (assigning/revoking partition state cleanly when consumer group membership changes). | Pending |
| **Day 8** | BigQuery incremental `MERGE` loader and local Parquet partition compactor (`year=.../month=.../day=...`). | Pending |
| **Day 9** | Out-of-order skew and hot-partition stress harness (Zipfian account traffic + straggler partition watermark advancement). | Pending |
| **Day 10** | Real-time anomaly and velocity detector (sliding-window refund spikes and high-frequency authorization alerts). | Pending |
| **Day 11** | Incremental SQL materialized views for hourly/daily cohort retention and net revenue reconciliation. | Pending |
| **Day 12** | Backpressure and bounded in-memory queue control between Kafka fetcher threads and DuckDB/Parquet flush workers. | Pending |
| **Day 13** | End-to-end Docker Compose integration test against a live Redpanda/Kafka broker container. | Pending |
| **Day 14** | Late-data side-output reconciliation (updating historical warehouse partitions with bounded retraction records). | Pending |
| **Day 15** | Observability and health metrics endpoint (`/metrics` and `/healthz` exposing watermark lag, DLQ rate, and sink latency). | Pending |
| **Day 16** | Multi-worker parallel partition execution benchmark (scaling across 1, 2, 4, and 8 cores). | Pending |
| **Day 17** | Data quality SLA monitor (automated freshness, volume, and null-rate assertions on warehouse tables). | Pending |
| **Day 18** | Fault-injection test suite (simulated sink write failures, partial batch rollbacks, and corrupt JSON payloads). | Pending |
| **Day 19** | Cost-optimized BigQuery storage layout benchmark (comparing clustered vs. unclustered scan bytes). | Pending |
| **Day 20** | Fresh-clone reproducibility verification and interactive CLI walkthrough. | Pending |
| **Day 21** | Final release audit, trade-off analysis, and measured benchmark portfolio report. | Pending |

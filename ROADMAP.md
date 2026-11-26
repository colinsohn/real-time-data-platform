# 21-Day Development Roadmap (`real-time-data-platform`)

Timeline: **November 26 – December 16, 2026** (Active New Project Slot 1/2).

| Day | Date | Status | Milestone | Verification Command |
| --- | --- | :---: | --- | --- |
| **Day 1** | Nov 26 | `[x]` | **Event Contract, Partitioned Kafka Log Engine, SQL Warehouse Loader & Baseline Benchmark**: Versioned v1.0 schema validator, structured dead-letter quarantine (`events.dlq.v1`), Kafka `murmur2` partitioner, CRC32-verified partitioned topic log with consumer-group offset commits, DuckDB SQL micro-batch deduplication & hourly rollup loader, BigQuery partitioned/clustered DDL & `MERGE` compiler, Docker setup, unit/integration tests, and 10k-event benchmark (`reports/benchmark_day01.json`). | `pytest -v && python3 -B -m rt_data_platform.cli benchmark --output reports/benchmark_day01.json` |
| **Day 2** | Nov 27 | `[ ]` | **Schema Evolution Registry & Dead-Letter Replay Metadata**: Backward/full schema compatibility validator and structured DLQ inspection queries. | `pytest -v` |
| **Day 3** | Nov 28 | `[ ]` | **Event-Time Watermarking & Bounded Out-of-Orderness**: Per-partition event-time watermark tracker and late-event side-output routing. | `pytest -v` |
| **Day 4** | Nov 29 | `[ ]` | **Two-Phase Checkpoint Manifest & Idempotent Batch Ledger**: Persistent batch commit ledger recording `(batch_id, partition, start_offset, end_offset, sha256)` inside the warehouse transaction. | `pytest -v` |
| **Day 5** | Nov 30 | `[ ]` | **Tumbling & Sliding Window SQL Aggregations**: 1-minute, 5-minute, and 1-hour windowed aggregations with batch-SQL parity checks. | `pytest -v` |
| **Day 6** | Dec 1 | `[ ]` | **Session Windowization & Out-of-Order Session Merging**: Dynamic inactivity-gap sessionizer merging split intervals when out-of-order bridge events arrive. | `pytest -v` |
| **Day 7** | Dec 2 | `[ ]` | **BigQuery Partition-Pruned MERGE Optimizer**: Partition-filter pushdown (`WHERE target.event_date IN (...)`) reducing BigQuery `MERGE` bytes scanned. | `pytest -v` |
| **Day 8** | Dec 3 | `[ ]` | **Columnar Parquet Micro-Batch Sink & Compression Audit**: Batched Parquet export with SHA-256 manifest verification. | `pytest -v` |
| **Day 9** | Dec 4 | `[ ]` | **Consumer Group Rebalance & Partition Revocation Simulation**: Cooperative sticky partition assignment and pre-revocation flush hooks. | `pytest -v` |
| **Day 10** | Dec 5 | `[ ]` | **Backpressure, Rate Limiting & Bounded Prefetch Queues**: Token-bucket producer throttling and lag-driven consumer backpressure. | `pytest -v` |
| **Day 11** | Dec 6 | `[ ]` | **Data Quality Assertions & Drift Monitors**: SQL batch quality gates blocking staging promotion on anomaly thresholds. | `pytest -v` |
| **Day 12** | Dec 7 | `[ ]` | **Dead-Letter Repair & Replay CLI**: Transactional repair and re-ingestion workflow for quarantined `events.dlq.v1` payloads. | `pytest -v` |
| **Day 13** | Dec 8 | `[ ]` | **Multi-Container Docker Compose Redpanda/Kafka Integration Harness**: Containerized end-to-end smoke verification against Redpanda/Kafka. | `docker compose config && pytest -v` |
| **Day 14** | Dec 9 | `[ ]` | **Crash-Injection & Offset-Rollback Chaos Harness**: Multi-worker fault-injection suite verifying zero duplicate or lost cents under random worker kills. | `pytest -v` |
| **Day 15** | Dec 10 | `[ ]` | **Point-in-Time SCD Type 2 Dimension Enrichment**: Temporal SQL join enriching streaming events with historical customer tier dimensions. | `pytest -v` |
| **Day 16** | Dec 11 | `[ ]` | **Pipeline Observability, Consumer Lag & Watermark Skew Endpoints**: JSON diagnostics and Prometheus-compatible metrics exporter. | `pytest -v` |
| **Day 17** | Dec 12 | `[ ]` | **BigQuery Storage Write API vs. GCS Parquet Load Cost Model**: Quantitative cost and latency analysis in `DEPLOYMENT.md` (zero live spend). | `pytest -v` |
| **Day 18** | Dec 13 | `[ ]` | **Vectorized Batch Ingestion & Profiling Optimization**: Profile and optimize batch ingestion path with measured before/after benchmark. | `pytest -v` |
| **Day 19** | Dec 14 | `[ ]` | **Multi-Tenant Zipfian Skew & Hot-Partition Stress Benchmark**: Measure partition skew and throughput under uniform vs. power-law tenant traffic. | `pytest -v` |
| **Day 20** | Dec 15 | `[ ]` | **Fresh-Clone Reproduction & End-to-End Walkthrough**: Single-command fresh-clone verification and operational walkthrough. | `pytest -v` |
| **Day 21** | Dec 16 | `[ ]` | **Final Release Audit & Capacity Transition**: Verify all acceptance criteria and release concurrency slot for `distributed-kv-store`. | `pytest -v` |

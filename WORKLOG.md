# Work Log (`real-time-data-platform`)

## November 26, 2026 — Day 1: Event Contract, Partitioned Kafka Log Engine, SQL Warehouse Loader & Baseline Benchmark

- Initialized `real-time-data-platform` (Active New Project Slot 1/2) with `ARCHITECTURE.md`, 21-day `ROADMAP.md`, `AGENTS.md`, `WORKLOG.md`, `Dockerfile`, `docker-compose.yml`, `sql/bigquery_schema.sql`, and `.github/workflows/ci.yml`.
- Implemented versioned v1.0 schema contract validator and structured dead-letter router (`src/rt_data_platform/contract.py`), Apache Kafka-compatible 32-bit `murmur2` key partitioner and CRC32-verified partitioned topic log with consumer-group atomic offset commits (`src/rt_data_platform/kafka_log.py`), DuckDB/SQLite micro-batch deduplicating warehouse loader and BigQuery partitioned/clustered DDL & `MERGE` compiler (`src/rt_data_platform/warehouse.py`), end-to-end streaming pipeline (`src/rt_data_platform/pipeline.py`), and CLI demo + seeded benchmark runner (`src/rt_data_platform/cli.py`).
- Added comprehensive unit and integration tests (`tests/test_platform.py`) covering UTC timestamp normalization, dead-letter quarantine reason codes, murmur2 partition determinism, CRC32 disk corruption detection, intra-batch and cross-batch duplicate deduplication, crash-before-offset-commit recovery, BigQuery DDL/MERGE compilation, and financial conservation.

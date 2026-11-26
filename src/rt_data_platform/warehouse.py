"""SQL Warehouse Loader (DuckDB / SQLite execution) and BigQuery Standard SQL DDL/MERGE compiler."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional, Sequence

from .kafka_log import KafkaRecord

try:
    import duckdb  # type: ignore

    HAS_DUCKDB = True
except ImportError:  # pragma: no cover
    duckdb = None  # type: ignore
    HAS_DUCKDB = False


@dataclass(frozen=True)
class BatchLoadResult:
    """Metrics returned from loading and deduplicating a single Kafka micro-batch."""

    batch_id: str
    records_received: int
    new_unique_events_inserted: int
    duplicates_dropped: int
    next_offsets: Dict[int, int]


class BigQueryCompiler:
    """Generates production BigQuery Standard SQL DDL and idempotent staging MERGE queries."""

    def __init__(self, project_id: str, dataset_id: str) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id

    def fq_table(self, table_name: str) -> str:
        return f"`{self.project_id}.{self.dataset_id}.{table_name}`"

    def compile_core_ddl(self) -> str:
        core = self.fq_table("core_telemetry_events")
        mart = self.fq_table("mart_tenant_hourly_metrics")
        return f"""-- BigQuery Partitioned & Clustered Core Events Table
CREATE TABLE IF NOT EXISTS {core} (
  event_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  tenant_id STRING NOT NULL,
  event_type STRING NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  event_ts_ms INT64 NOT NULL,
  event_date DATE NOT NULL,
  event_hour_utc INT64 NOT NULL,
  session_id STRING NOT NULL,
  amount_cents INT64 NOT NULL,
  latency_ms FLOAT64 NOT NULL,
  attributes_json JSON,
  kafka_topic STRING NOT NULL,
  kafka_partition INT64 NOT NULL,
  kafka_offset INT64 NOT NULL,
  batch_id STRING NOT NULL
)
PARTITION BY event_date
CLUSTER BY tenant_id, event_type
OPTIONS (
  require_partition_filter = TRUE,
  description = 'Deduplicated streaming telemetry events keyed by event_id'
);

-- BigQuery Incremental Hourly Tenant Metrics Mart
CREATE TABLE IF NOT EXISTS {mart} (
  event_date DATE NOT NULL,
  event_hour_utc INT64 NOT NULL,
  tenant_id STRING NOT NULL,
  event_type STRING NOT NULL,
  event_count INT64 NOT NULL,
  distinct_sessions INT64 NOT NULL,
  total_amount_cents INT64 NOT NULL,
  avg_latency_ms FLOAT64 NOT NULL,
  max_latency_ms FLOAT64 NOT NULL,
  min_event_ts STRING NOT NULL,
  max_event_ts STRING NOT NULL
)
PARTITION BY event_date
CLUSTER BY tenant_id, event_type;
"""

    def compile_idempotent_merge(self, staging_table: str = "stg_telemetry_events") -> str:
        core = self.fq_table("core_telemetry_events")
        stg = self.fq_table(staging_table)
        return f"""-- Idempotent At-Least-Once Deduplicating MERGE into BigQuery Core Table
MERGE INTO {core} AS target
USING (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY kafka_partition ASC, kafka_offset ASC
      ) AS row_num
    FROM {stg}
  )
  WHERE row_num = 1
) AS source
ON target.event_id = source.event_id
   AND target.event_date = source.event_date
WHEN NOT MATCHED THEN
  INSERT (
    event_id, schema_version, tenant_id, event_type, event_ts, event_ts_ms,
    event_date, event_hour_utc, session_id, amount_cents, latency_ms,
    attributes_json, kafka_topic, kafka_partition, kafka_offset, batch_id
  )
  VALUES (
    source.event_id, source.schema_version, source.tenant_id, source.event_type,
    source.event_ts, source.event_ts_ms, source.event_date, source.event_hour_utc,
    source.session_id, source.amount_cents, source.latency_ms,
    source.attributes_json, source.kafka_topic, source.kafka_partition,
    source.kafka_offset, source.batch_id
  );
"""


class SQLWarehouseLoader:
    """Analytical SQL warehouse loader executing staging deduplication and hourly mart rollups."""

    def __init__(self, db_path: Optional[Path] = None, prefer_duckdb: bool = True) -> None:
        self.use_duckdb = bool(prefer_duckdb and HAS_DUCKDB)
        if self.use_duckdb:
            target = str(db_path) if db_path is not None else ":memory:"
            self.conn = duckdb.connect(target)
        else:
            target = str(db_path) if db_path is not None else ":memory:"
            self.conn = sqlite3.connect(target)
        self._init_schema()

    @property
    def engine_name(self) -> str:
        return "duckdb" if self.use_duckdb else "sqlite3"

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stg_telemetry_events (
                batch_id VARCHAR NOT NULL,
                event_id VARCHAR NOT NULL,
                schema_version VARCHAR NOT NULL,
                tenant_id VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                event_ts VARCHAR NOT NULL,
                event_ts_ms BIGINT NOT NULL,
                event_date VARCHAR NOT NULL,
                event_hour_utc INTEGER NOT NULL,
                session_id VARCHAR NOT NULL,
                amount_cents BIGINT NOT NULL,
                latency_ms DOUBLE NOT NULL,
                attributes_json VARCHAR NOT NULL,
                kafka_topic VARCHAR NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset BIGINT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS core_telemetry_events (
                event_id VARCHAR PRIMARY KEY,
                schema_version VARCHAR NOT NULL,
                tenant_id VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                event_ts VARCHAR NOT NULL,
                event_ts_ms BIGINT NOT NULL,
                event_date VARCHAR NOT NULL,
                event_hour_utc INTEGER NOT NULL,
                session_id VARCHAR NOT NULL,
                amount_cents BIGINT NOT NULL,
                latency_ms DOUBLE NOT NULL,
                attributes_json VARCHAR NOT NULL,
                kafka_topic VARCHAR NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset BIGINT NOT NULL,
                batch_id VARCHAR NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mart_tenant_hourly_metrics (
                event_date VARCHAR NOT NULL,
                event_hour_utc INTEGER NOT NULL,
                tenant_id VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                event_count BIGINT NOT NULL,
                distinct_sessions BIGINT NOT NULL,
                total_amount_cents BIGINT NOT NULL,
                avg_latency_ms DOUBLE NOT NULL,
                max_latency_ms DOUBLE NOT NULL,
                min_event_ts VARCHAR NOT NULL,
                max_event_ts VARCHAR NOT NULL,
                PRIMARY KEY (event_date, event_hour_utc, tenant_id, event_type)
            )
            """
        )

    def load_micro_batch(self, batch_id: str, records: Sequence[KafkaRecord]) -> BatchLoadResult:
        """Stage a Kafka micro-batch, deduplicate by event_id in SQL, and refresh the hourly mart."""
        if not records:
            return BatchLoadResult(
                batch_id=batch_id,
                records_received=0,
                new_unique_events_inserted=0,
                duplicates_dropped=0,
                next_offsets={},
            )

        rows = []
        next_offsets: Dict[int, int] = {}
        for rec in records:
            val = rec.value
            rows.append(
                (
                    batch_id,
                    val["event_id"],
                    val["schema_version"],
                    val["tenant_id"],
                    val["event_type"],
                    val["event_ts"],
                    int(val["event_ts_ms"]),
                    val["event_date"],
                    int(val["event_hour_utc"]),
                    val["session_id"],
                    int(val["amount_cents"]),
                    float(val["latency_ms"]),
                    val["attributes_json"],
                    rec.topic,
                    int(rec.partition),
                    int(rec.offset),
                )
            )
            next_offsets[rec.partition] = max(
                next_offsets.get(rec.partition, 0), rec.offset + 1
            )

        before_count = self.core_event_count()

        self.conn.execute("DELETE FROM stg_telemetry_events WHERE batch_id = ?", (batch_id,))
        chunk_size = 250
        for start in range(0, len(rows), chunk_size):
            chunk = rows[start : start + chunk_size]
            placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(chunk))
            flat_params = [item for tup in chunk for item in tup]
            self.conn.execute(
                f"""
                INSERT INTO stg_telemetry_events (
                    batch_id, event_id, schema_version, tenant_id, event_type,
                    event_ts, event_ts_ms, event_date, event_hour_utc, session_id,
                    amount_cents, latency_ms, attributes_json, kafka_topic,
                    kafka_partition, kafka_offset
                ) VALUES {placeholders}
                """,
                flat_params,
            )

        # Deduplicate intra-batch via ROW_NUMBER() and cross-batch via NOT EXISTS on core_telemetry_events
        self.conn.execute(
            """
            INSERT INTO core_telemetry_events (
                event_id, schema_version, tenant_id, event_type, event_ts,
                event_ts_ms, event_date, event_hour_utc, session_id, amount_cents,
                latency_ms, attributes_json, kafka_topic, kafka_partition,
                kafka_offset, batch_id
            )
            SELECT
                s.event_id, s.schema_version, s.tenant_id, s.event_type, s.event_ts,
                s.event_ts_ms, s.event_date, s.event_hour_utc, s.session_id, s.amount_cents,
                s.latency_ms, s.attributes_json, s.kafka_topic, s.kafka_partition,
                s.kafka_offset, s.batch_id
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY event_id
                        ORDER BY kafka_partition ASC, kafka_offset ASC
                    ) AS rn
                FROM stg_telemetry_events
                WHERE batch_id = ?
            ) AS s
            WHERE s.rn = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM core_telemetry_events AS c
                  WHERE c.event_id = s.event_id
              )
            """,
            (batch_id,),
        )

        # Rebuild affected dates in mart_tenant_hourly_metrics
        self.conn.execute("DELETE FROM mart_tenant_hourly_metrics")
        self.conn.execute(
            """
            INSERT INTO mart_tenant_hourly_metrics (
                event_date, event_hour_utc, tenant_id, event_type,
                event_count, distinct_sessions, total_amount_cents,
                avg_latency_ms, max_latency_ms, min_event_ts, max_event_ts
            )
            SELECT
                event_date,
                event_hour_utc,
                tenant_id,
                event_type,
                COUNT(*) AS event_count,
                COUNT(DISTINCT session_id) AS distinct_sessions,
                 COALESCE(SUM(amount_cents), 0) AS total_amount_cents,
                ROUND(AVG(latency_ms), 3) AS avg_latency_ms,
                ROUND(MAX(latency_ms), 3) AS max_latency_ms,
                MIN(event_ts) AS min_event_ts,
                MAX(event_ts) AS max_event_ts
            FROM core_telemetry_events
            GROUP BY event_date, event_hour_utc, tenant_id, event_type
            """
        )
        self.conn.execute("DELETE FROM stg_telemetry_events WHERE batch_id = ?", (batch_id,))
        if not self.use_duckdb:
            self.conn.commit()

        after_count = self.core_event_count()
        inserted = after_count - before_count
        duplicates = len(records) - inserted

        return BatchLoadResult(
            batch_id=batch_id,
            records_received=len(records),
            new_unique_events_inserted=inserted,
            duplicates_dropped=duplicates,
            next_offsets=next_offsets,
        )

    def core_event_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM core_telemetry_events").fetchone()
        return int(row[0]) if row else 0

    def total_amount_cents(self) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(amount_cents), 0) FROM core_telemetry_events"
        ).fetchone()
        return int(row[0]) if row else 0

    def fetch_hourly_mart(self) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT
                event_date, event_hour_utc, tenant_id, event_type,
                event_count, distinct_sessions, total_amount_cents,
                avg_latency_ms, max_latency_ms, min_event_ts, max_event_ts
            FROM mart_tenant_hourly_metrics
            ORDER BY event_date ASC, event_hour_utc ASC, tenant_id ASC, event_type ASC
            """
        ).fetchall()
        cols = [
            "event_date",
            "event_hour_utc",
            "tenant_id",
            "event_type",
            "event_count",
            "distinct_sessions",
            "total_amount_cents",
            "avg_latency_ms",
            "max_latency_ms",
            "min_event_ts",
            "max_event_ts",
        ]
        return [dict(zip(cols, r)) for r in rows]

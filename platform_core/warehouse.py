"""Analytical SQL warehouse sink backed by DuckDB with BigQuery-compatible schema and audit checks."""
import json
from pathlib import Path
import tempfile
from typing import Any, Dict, Iterable, List, Tuple

import duckdb

from platform_core.processor import DeadLetterRecord, WindowAggregate
from platform_core.schema import StreamEvent


class WarehouseSink:
    """Idempotent analytical SQL warehouse sink for raw events, window rollups, and DLQ records."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = str(db_path)
        self.conn = duckdb.connect(self.db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS commerce_events_raw (
                event_id VARCHAR PRIMARY KEY,
                event_type VARCHAR NOT NULL,
                account_id VARCHAR NOT NULL,
                occurred_at_ms BIGINT NOT NULL,
                ingested_at_ms BIGINT NOT NULL,
                kafka_partition INTEGER NOT NULL,
                amount_cents BIGINT NOT NULL,
                signed_amount_cents BIGINT NOT NULL,
                currency VARCHAR NOT NULL,
                schema_version INTEGER NOT NULL,
                payload_json VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS account_minute_rollups (
                window_start_ms BIGINT NOT NULL,
                window_end_ms BIGINT NOT NULL,
                account_id VARCHAR NOT NULL,
                kafka_partition INTEGER NOT NULL,
                event_count BIGINT NOT NULL,
                order_count BIGINT NOT NULL,
                payment_authorized_count BIGINT NOT NULL,
                refund_count BIGINT NOT NULL,
                gross_volume_cents BIGINT NOT NULL,
                refund_volume_cents BIGINT NOT NULL,
                net_volume_cents BIGINT NOT NULL,
                max_event_time_ms BIGINT NOT NULL,
                watermark_ms BIGINT NOT NULL,
                PRIMARY KEY (window_start_ms, window_end_ms, account_id)
            );

            CREATE TABLE IF NOT EXISTS dlq_events (
                dlq_id BIGINT,
                category VARCHAR NOT NULL,
                reason VARCHAR NOT NULL,
                observed_at_ms BIGINT NOT NULL,
                raw_payload VARCHAR NOT NULL
            );
        """)

    def _bulk_json_upsert(self, table: str, columns_spec: str, records: List[Dict[str, Any]], replace: bool = True) -> int:
        if not records:
            return 0
        verb = "INSERT OR REPLACE INTO" if replace else "INSERT INTO"
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", encoding="utf-8", delete=True) as tmp:
            for item in records:
                tmp.write(json.dumps(item, separators=(",", ":")) + "\n")
            tmp.flush()
            self.conn.execute(
                f"{verb} {table} SELECT * FROM read_json(?, format='newline_delimited', columns={columns_spec})",
                [tmp.name],
            )
        return len(records)

    def sink_events(self, events: Iterable[Tuple[StreamEvent, int]]) -> int:
        records = [
            {
                "event_id": ev.event_id,
                "event_type": ev.event_type,
                "account_id": ev.account_id,
                "occurred_at_ms": ev.occurred_at_ms,
                "ingested_at_ms": ev.ingested_at_ms,
                "kafka_partition": part,
                "amount_cents": ev.amount_cents,
                "signed_amount_cents": ev.signed_amount_cents(),
                "currency": ev.currency,
                "schema_version": ev.schema_version,
                "payload_json": json.dumps(ev.payload, sort_keys=True),
            }
            for ev, part in events
        ]
        cols = (
            "{'event_id':'VARCHAR','event_type':'VARCHAR','account_id':'VARCHAR',"
            "'occurred_at_ms':'BIGINT','ingested_at_ms':'BIGINT','kafka_partition':'INTEGER',"
            "'amount_cents':'BIGINT','signed_amount_cents':'BIGINT','currency':'VARCHAR',"
            "'schema_version':'INTEGER','payload_json':'VARCHAR'}"
        )
        return self._bulk_json_upsert("commerce_events_raw", cols, records, replace=True)

    def sink_rollups(self, rollups: Iterable[WindowAggregate]) -> int:
        records = [
            {
                "window_start_ms": w.window_start_ms,
                "window_end_ms": w.window_end_ms,
                "account_id": w.account_id,
                "kafka_partition": w.kafka_partition,
                "event_count": w.event_count,
                "order_count": w.order_count,
                "payment_authorized_count": w.payment_authorized_count,
                "refund_count": w.refund_count,
                "gross_volume_cents": w.gross_volume_cents,
                "refund_volume_cents": w.refund_volume_cents,
                "net_volume_cents": w.net_volume_cents,
                "max_event_time_ms": w.max_event_time_ms,
                "watermark_ms": w.watermark_ms,
            }
            for w in rollups
        ]
        cols = (
            "{'window_start_ms':'BIGINT','window_end_ms':'BIGINT','account_id':'VARCHAR',"
            "'kafka_partition':'INTEGER','event_count':'BIGINT','order_count':'BIGINT',"
            "'payment_authorized_count':'BIGINT','refund_count':'BIGINT','gross_volume_cents':'BIGINT',"
            "'refund_volume_cents':'BIGINT','net_volume_cents':'BIGINT','max_event_time_ms':'BIGINT',"
            "'watermark_ms':'BIGINT'}"
        )
        return self._bulk_json_upsert("account_minute_rollups", cols, records, replace=True)

    def sink_dlq(self, records: Iterable[DeadLetterRecord]) -> int:
        base_id = self.conn.execute("SELECT coalesce(max(dlq_id), 0) FROM dlq_events").fetchone()[0]
        items = [
            {
                "dlq_id": int(base_id + idx + 1),
                "category": rec.category,
                "reason": rec.reason,
                "observed_at_ms": rec.observed_at_ms,
                "raw_payload": rec.raw_payload,
            }
            for idx, rec in enumerate(records)
        ]
        cols = "{'dlq_id':'BIGINT','category':'VARCHAR','reason':'VARCHAR','observed_at_ms':'BIGINT','raw_payload':'VARCHAR'}"
        return self._bulk_json_upsert("dlq_events", cols, items, replace=False)

    def verify_reconciliation(self, processor_stats: Dict[str, int]) -> Dict[str, Any]:
        """Prove conservation of event counts and exact dollar/cent totals between raw and rollup tables."""
        raw_row = self.conn.execute("""
            SELECT
                count(*) AS raw_count,
                coalesce(sum(CASE WHEN event_type = 'order_created' THEN amount_cents ELSE 0 END), 0) AS raw_gross_cents,
                coalesce(sum(CASE WHEN event_type = 'refund_issued' THEN amount_cents ELSE 0 END), 0) AS raw_refund_cents,
                coalesce(sum(signed_amount_cents), 0) AS raw_net_cents
            FROM commerce_events_raw
        """).fetchone()

        rollup_row = self.conn.execute("""
            SELECT
                count(*) AS window_rows,
                coalesce(sum(event_count), 0) AS rollup_event_count,
                coalesce(sum(gross_volume_cents), 0) AS rollup_gross_cents,
                coalesce(sum(refund_volume_cents), 0) AS rollup_refund_cents,
                coalesce(sum(net_volume_cents), 0) AS rollup_net_cents
            FROM account_minute_rollups
        """).fetchone()

        dlq_count = self.conn.execute("SELECT count(*) FROM dlq_events").fetchone()[0]

        expected_total = (
            processor_stats["accepted_unique"]
            + processor_stats["duplicates_dropped"]
            + processor_stats["late_events_dlq"]
            + processor_stats["invalid_events_dlq"]
        )
        conserved = (
            processor_stats["total_received"] == expected_total
            and raw_row[0] == processor_stats["accepted_unique"]
            and rollup_row[1] == raw_row[0]
            and rollup_row[2] == raw_row[1]
            and rollup_row[3] == raw_row[2]
            and rollup_row[4] == raw_row[3]
            and dlq_count == processor_stats["total_dlq"]
        )
        if not conserved:
            raise AssertionError("warehouse reconciliation conservation check failed")

        return {
            "conservation_verified": True,
            "raw_events_stored": int(raw_row[0]),
            "window_rollups_stored": int(rollup_row[0]),
            "dlq_records_stored": int(dlq_count),
            "gross_volume_cents": int(rollup_row[2]),
            "refund_volume_cents": int(rollup_row[3]),
            "net_volume_cents": int(rollup_row[4]),
        }

    def top_accounts_summary(self, limit: int = 5) -> List[Dict[str, Any]]:
        cursor = self.conn.execute(
            """
            SELECT
                account_id,
                count(*) AS windows_active,
                sum(event_count) AS total_events,
                sum(order_count) AS orders,
                sum(refund_count) AS refunds,
                sum(gross_volume_cents) AS gross_cents,
                sum(net_volume_cents) AS net_cents
            FROM account_minute_rollups
            GROUP BY account_id
            ORDER BY net_cents DESC, account_id ASC
            LIMIT ?
            """,
            [limit],
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, (int(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v for v in r))) for r in cursor.fetchall()]

    def export_bigquery_jsonl(self, destination_dir: Path) -> Dict[str, int]:
        destination_dir = Path(destination_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)
        rollups_path = destination_dir / "account_minute_rollups.jsonl"
        cursor = self.conn.execute("SELECT * FROM account_minute_rollups ORDER BY window_start_ms, account_id")
        cols = [d[0] for d in cursor.description]
        count = 0
        with rollups_path.open("w", encoding="utf-8") as stream:
            for row in cursor.fetchall():
                stream.write(json.dumps(dict(zip(cols, row)), sort_keys=True) + "\n")
                count += 1
        return {"account_minute_rollups_rows": count}

    def close(self) -> None:
        self.conn.close()

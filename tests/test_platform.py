from pathlib import Path
import tempfile
import unittest

from benchmark import run_benchmark
from cmd_demo import run_demo
from platform_core.processor import StatefulStreamProcessor
from platform_core.schema import (
    SchemaValidationError,
    bigquery_ddl,
    deterministic_partition,
    validate_event,
)
from platform_core.warehouse import WarehouseSink


def sample_raw(
    event_id="evt-1",
    event_type="order_created",
    account_id="acct-1",
    occurred_at_ms=100_000,
    ingested_at_ms=100_100,
    amount_cents=1500,
):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "account_id": account_id,
        "occurred_at_ms": occurred_at_ms,
        "ingested_at_ms": ingested_at_ms,
        "amount_cents": amount_cents,
        "currency": "USD",
        "schema_version": 1,
        "payload": {"order_id": "ord-1"},
    }


class PlatformTests(unittest.TestCase):
    def test_schema_validation_and_deterministic_partitioning(self):
        ev, part = validate_event(sample_raw(), num_partitions=16)
        self.assertEqual(ev.event_id, "evt-1")
        self.assertEqual(ev.signed_amount_cents(), 1500)
        self.assertEqual(part, deterministic_partition("acct-1", 16))
        self.assertTrue(0 <= part < 16)

        refund, _ = validate_event(sample_raw(event_id="evt-2", event_type="refund_issued", amount_cents=400))
        self.assertEqual(refund.signed_amount_cents(), -400)

        for bad_update in (
            {"event_id": ""},
            {"event_type": "unknown"},
            {"account_id": "   "},
            {"occurred_at_ms": -1},
            {"occurred_at_ms": True},
            {"amount_cents": -5},
            {"currency": "EUR"},
            {"schema_version": 2},
            {"payload": "not-a-dict"},
        ):
            raw = sample_raw()
            raw.update(bad_update)
            with self.assertRaises(SchemaValidationError):
                validate_event(raw)

        ddl = bigquery_ddl("proj-1", "dataset_1")
        self.assertIn("PARTITION BY DATE(occurred_at)", ddl)
        self.assertIn("CLUSTER BY account_id, event_type", ddl)

    def test_deduplication_watermark_window_closure_and_late_dlq(self):
        proc = StatefulStreamProcessor(window_size_ms=60_000, max_out_of_orderness_ms=10_000, num_partitions=1)
        self.assertEqual(proc.ingest(sample_raw("e1", "order_created", "acct-1", 10_000, 10_050, 2000)), "accepted")
        # Duplicate event_id is dropped without double-counting
        self.assertEqual(proc.ingest(sample_raw("e1", "order_created", "acct-1", 10_000, 10_200, 2000)), "duplicate")
        # Out-of-order refund inside [0, 60_000)
        self.assertEqual(proc.ingest(sample_raw("e2", "refund_issued", "acct-1", 45_000, 45_100, 500)), "accepted")
        self.assertEqual(len(proc.closed_windows), 0)

        # Event at 75_000 advances watermark to 65_000 >= 60_000, closing window [0, 60_000)
        self.assertEqual(proc.ingest(sample_raw("e3", "order_created", "acct-1", 75_000, 75_100, 1000)), "accepted")
        self.assertEqual(len(proc.closed_windows), 1)
        closed = proc.closed_windows[0]
        self.assertEqual(closed.window_start_ms, 0)
        self.assertEqual(closed.window_end_ms, 60_000)
        self.assertEqual(closed.event_count, 2)
        self.assertEqual(closed.gross_volume_cents, 2000)
        self.assertEqual(closed.refund_volume_cents, 500)
        self.assertEqual(closed.net_volume_cents, 1500)

        # Late arrival for closed window [0, 60_000) is routed to DLQ
        self.assertEqual(proc.ingest(sample_raw("e4-late", "order_created", "acct-1", 20_000, 76_000, 9999)), "late")
        self.assertEqual(proc.stats()["late_events_dlq"], 1)

    def test_warehouse_idempotent_upsert_reconciliation_and_jsonl_export(self):
        demo = run_demo()
        self.assertTrue(demo["warehouse_reconciliation"]["conservation_verified"])
        self.assertEqual(demo["processor_stats"]["accepted_unique"], 5)
        self.assertEqual(demo["processor_stats"]["duplicates_dropped"], 1)
        self.assertEqual(demo["processor_stats"]["late_events_dlq"], 1)
        self.assertEqual(demo["processor_stats"]["invalid_events_dlq"], 1)
        self.assertEqual(demo["warehouse_reconciliation"]["net_volume_cents"], 72_500)

        proc = StatefulStreamProcessor(window_size_ms=60_000, max_out_of_orderness_ms=5_000, num_partitions=4)
        proc.ingest(sample_raw("e1", "order_created", "acct-A", 15_000, 15_010, 3000))
        proc.flush_all()

        with tempfile.TemporaryDirectory() as tmp:
            sink = WarehouseSink(str(Path(tmp) / "warehouse.duckdb"))
            sink.sink_events(proc.accepted_events)
            # Re-sinking identical events and rollups is idempotent via INSERT OR REPLACE
            sink.sink_events(proc.accepted_events)
            sink.sink_rollups(proc.closed_windows)
            sink.sink_rollups(proc.closed_windows)
            rec = sink.verify_reconciliation(proc.stats())
            self.assertEqual(rec["raw_events_stored"], 1)
            self.assertEqual(rec["window_rollups_stored"], 1)
            exported = sink.export_bigquery_jsonl(Path(tmp) / "bq_export")
            self.assertEqual(exported["account_minute_rollups_rows"], 1)
            sink.close()

    def test_benchmark_conservation(self):
        report = run_benchmark(num_events=2_000, batch_size=500)
        self.assertTrue(report["warehouse_reconciliation"]["conservation_verified"])
        self.assertGreater(report["throughput"]["end_to_end_events_per_second"], 1000.0)


if __name__ == "__main__":
    unittest.main()

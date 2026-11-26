"""Unit and integration test suite for real-time-data-platform."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from benchmark import run_benchmark as run_watermarked_benchmark
from cmd_demo import run_demo as run_watermarked_demo
from rt_data_platform.cli import run_benchmark
from rt_data_platform.contract import validate_raw_event
from rt_data_platform.kafka_log import PartitionedTopicLog, kafka_murmur2, partition_for_key
from rt_data_platform.pipeline import StreamingPipeline


class TestRealTimeDataPlatform(unittest.TestCase):
    def test_valid_event_canonicalization_and_utc_normalization(self) -> None:
        raw = {
            "schema_version": "1.0",
            "event_id": "evt-100",
            "tenant_id": "tenant-alpha",
            "event_type": "payment_captured",
            "event_ts": "2026-11-25T18:30:00-08:00",
            "session_id": "sess-01",
            "amount_cents": 2599,
            "latency_ms": 14.25,
            "attributes": {"plan": "pro", "attempt": 1},
        }
        event, dlq = validate_raw_event(raw, ingested_at_ms=1795651200000)
        self.assertIsNone(dlq)
        assert event is not None
        self.assertEqual(event.event_id, "evt-100")
        self.assertEqual(event.event_ts, "2026-11-26T02:30:00.000Z")
        self.assertEqual(event.event_date, "2026-11-26")
        self.assertEqual(event.event_hour_utc, 2)
        self.assertEqual(event.amount_cents, 2599)

    def test_invalid_events_route_to_structured_dead_letter_records(self) -> None:
        cases = [
            ("{bad json", "MALFORMED_JSON"),
            (
                {
                    "schema_version": "2.0",
                    "event_id": "e1",
                    "tenant_id": "t1",
                    "event_type": "api_request",
                    "event_ts": "2026-11-26T00:00:00Z",
                    "session_id": "s1",
                },
                "UNSUPPORTED_SCHEMA_VERSION",
            ),
            (
                {
                    "schema_version": "1.0",
                    "event_id": "",
                    "tenant_id": "t1",
                    "event_type": "api_request",
                    "event_ts": "2026-11-26T00:00:00Z",
                    "session_id": "s1",
                },
                "MISSING_REQUIRED_FIELD",
            ),
            (
                {
                    "schema_version": "1.0",
                    "event_id": "e1",
                    "tenant_id": "t1",
                    "event_type": "not_an_event",
                    "event_ts": "2026-11-26T00:00:00Z",
                    "session_id": "s1",
                },
                "INVALID_EVENT_TYPE",
            ),
            (
                {
                    "schema_version": "1.0",
                    "event_id": "e1",
                    "tenant_id": "t1",
                    "event_type": "api_request",
                    "event_ts": "2026-11-26T00:00:00",  # missing timezone offset
                    "session_id": "s1",
                },
                "INVALID_TIMESTAMP",
            ),
            (
                {
                    "schema_version": "1.0",
                    "event_id": "e1",
                    "tenant_id": "t1",
                    "event_type": "payment_captured",
                    "event_ts": "2026-11-26T00:00:00Z",
                    "session_id": "s1",
                    "amount_cents": -5,
                },
                "INVALID_AMOUNT_CENTS",
            ),
        ]
        for payload, expected_code in cases:
            ev, dlq = validate_raw_event(payload, ingested_at_ms=1795651200000)
            self.assertIsNone(ev)
            assert dlq is not None
            self.assertEqual(dlq.reason_code, expected_code)

    def test_kafka_murmur2_partitioner_determinism(self) -> None:
        # Known Kafka murmur2 test vector consistency
        h1 = kafka_murmur2(b"tenant-alpha")
        h2 = kafka_murmur2(b"tenant-alpha")
        self.assertEqual(h1, h2)
        p = partition_for_key("tenant-alpha", 8)
        self.assertTrue(0 <= p < 8)
        for _ in range(25):
            self.assertEqual(partition_for_key("tenant-alpha", 8), p)

    def test_disk_backed_topic_log_persistence_and_crc32_corruption_detection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            log = PartitionedTopicLog("events.raw.v1", num_partitions=4, storage_dir=tmp_path)
            rec = log.produce("tenant-1", {"event_id": "e1", "v": 42}, timestamp_ms=1000)
            self.assertTrue(rec.verify_crc())
            log.commit_offsets("cg-1", {rec.partition: 1})

            # Reload from disk and verify offset and record integrity
            reloaded = PartitionedTopicLog("events.raw.v1", num_partitions=4, storage_dir=tmp_path)
            self.assertEqual(reloaded.committed_offset("cg-1", rec.partition), 1)
            self.assertEqual(reloaded.poll_uncommitted("cg-1"), [])
            self.assertEqual(len(reloaded.poll_uncommitted("cg-2")), 1)

            # Corrupt the JSONL file on disk and assert CRC32 check catches it
            part_file = tmp_path / f"events.raw.v1-p{rec.partition:02d}.jsonl"
            data = json.loads(part_file.read_text(encoding="utf-8").strip())
            data["value"]["v"] = 999
            part_file.write_text(json.dumps(data) + "\n", encoding="utf-8")

            with self.assertRaises(ValueError):
                PartitionedTopicLog("events.raw.v1", num_partitions=4, storage_dir=tmp_path)

    def test_end_to_end_deduplication_and_crash_before_offset_commit_recovery(self) -> None:
        pipeline = StreamingPipeline(num_partitions=4)
        e1 = {
            "schema_version": "1.0",
            "event_id": "evt-unique-1",
            "tenant_id": "tenant-a",
            "event_type": "payment_captured",
            "event_ts": "2026-11-26T03:15:00Z",
            "session_id": "sess-1",
            "amount_cents": 1500,
            "latency_ms": 10.0,
        }
        e2 = {
            "schema_version": "1.0",
            "event_id": "evt-unique-2",
            "tenant_id": "tenant-a",
            "event_type": "payment_captured",
            "event_ts": "2026-11-26T03:45:00Z",
            "session_id": "sess-2",
            "amount_cents": 2500,
            "latency_ms": 20.0,
        }
        # Ingest e1 twice (intra-batch duplicate), e2 once, and 1 malformed record
        summary = pipeline.ingest_raw_events([e1, e1, e2, "{invalid_json"])
        self.assertEqual(summary.valid_produced, 3)
        self.assertEqual(summary.dead_letter_produced, 1)

        # Simulate crash after warehouse write but before Kafka offset commit
        b1 = pipeline.process_next_micro_batch(max_records=10, commit_offsets=False)
        self.assertEqual(b1.records_received, 3)
        self.assertEqual(b1.new_unique_events_inserted, 2)
        self.assertEqual(b1.duplicates_dropped, 1)
        self.assertEqual(pipeline.diagnostics()["consumer_total_lag"], 3)

        # Re-run micro-batch after recovery: all 3 records are re-polled, 0 inserted, offsets committed
        b2 = pipeline.process_next_micro_batch(max_records=10, commit_offsets=True)
        self.assertEqual(b2.records_received, 3)
        self.assertEqual(b2.new_unique_events_inserted, 0)
        self.assertEqual(b2.duplicates_dropped, 3)
        self.assertEqual(pipeline.diagnostics()["consumer_total_lag"], 0)
        self.assertEqual(pipeline.warehouse.total_amount_cents(), 4000)

        mart = pipeline.warehouse.fetch_hourly_mart()
        self.assertEqual(len(mart), 1)
        self.assertEqual(mart[0]["event_count"], 2)
        self.assertEqual(mart[0]["distinct_sessions"], 2)
        self.assertEqual(mart[0]["total_amount_cents"], 4000)
        self.assertAlmostEqual(mart[0]["avg_latency_ms"], 15.0)

    def test_bigquery_ddl_and_idempotent_merge_compiler(self) -> None:
        pipeline = StreamingPipeline(
            num_partitions=4,
            gcp_project="colin-portfolio-gcp",
            bq_dataset="streaming_prod",
        )
        ddl = pipeline.bq_compiler.compile_core_ddl()
        merge_sql = pipeline.bq_compiler.compile_idempotent_merge()
        self.assertIn("PARTITION BY event_date", ddl)
        self.assertIn("CLUSTER BY tenant_id, event_type", ddl)
        self.assertIn("require_partition_filter = TRUE", ddl)
        self.assertIn("MERGE INTO `colin-portfolio-gcp.streaming_prod.core_telemetry_events`", merge_sql)
        self.assertIn("ROW_NUMBER() OVER (", merge_sql)

    def test_benchmark_runner_invariants(self) -> None:
        report = run_benchmark(
            num_unique_events=1000,
            num_partitions=4,
            duplicate_rate=0.15,
            invalid_rate=0.05,
            batch_size=250,
            seed=42,
        )
        self.assertTrue(report["verification"]["amount_conservation_verified"])
        self.assertEqual(report["verification"]["warehouse_unique_events"], 1000)
        self.assertEqual(report["verification"]["duplicates_dropped"], 150)
        self.assertEqual(report["verification"]["dead_letter_produced"], 50)
        self.assertEqual(report["verification"]["consumer_total_lag"], 0)

    def test_watermarked_stream_processor_and_duckdb_warehouse_sink(self) -> None:
        demo = run_watermarked_demo()
        self.assertTrue(demo["warehouse_reconciliation"]["conservation_verified"])
        self.assertEqual(demo["processor_stats"]["accepted_unique"], 5)
        self.assertEqual(demo["processor_stats"]["duplicates_dropped"], 1)
        self.assertEqual(demo["processor_stats"]["late_events_dlq"], 1)
        self.assertEqual(demo["processor_stats"]["invalid_events_dlq"], 1)
        self.assertEqual(demo["warehouse_reconciliation"]["net_volume_cents"], 72_500)

        bench_report = run_watermarked_benchmark(num_events=2_000, batch_size=500)
        self.assertTrue(bench_report["warehouse_reconciliation"]["conservation_verified"])
        self.assertGreater(bench_report["throughput"]["end_to_end_events_per_second"], 1000.0)


if __name__ == "__main__":
    unittest.main()

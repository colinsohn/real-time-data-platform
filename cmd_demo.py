"""Runnable demonstration of the real-time Kafka -> stateful window -> SQL warehouse pipeline."""
import json
from platform_core.processor import StatefulStreamProcessor
from platform_core.schema import bigquery_ddl
from platform_core.warehouse import WarehouseSink


def build_demo_stream():
    base_ms = 1_764_115_200_000  # 2025-11-26T00:00:00Z
    return [
        # Window [0, 60_000) for acct-enterprise-01
        {
            "event_id": "evt-001",
            "event_type": "order_created",
            "account_id": "acct-enterprise-01",
            "occurred_at_ms": base_ms + 5_000,
            "ingested_at_ms": base_ms + 5_120,
            "amount_cents": 25_000,
            "currency": "USD",
            "schema_version": 1,
            "payload": {"order_id": "ord-1001", "region": "us-west"},
        },
        # Exact duplicate retry from Kafka at-least-once delivery (must be dropped)
        {
            "event_id": "evt-001",
            "event_type": "order_created",
            "account_id": "acct-enterprise-01",
            "occurred_at_ms": base_ms + 5_000,
            "ingested_at_ms": base_ms + 5_400,
            "amount_cents": 25_000,
            "currency": "USD",
            "schema_version": 1,
            "payload": {"order_id": "ord-1001", "region": "us-west"},
        },
        # Out-of-order event still within 10s watermark tolerance
        {
            "event_id": "evt-002",
            "event_type": "refund_issued",
            "account_id": "acct-enterprise-01",
            "occurred_at_ms": base_ms + 42_000,
            "ingested_at_ms": base_ms + 42_100,
            "amount_cents": 5_000,
            "currency": "USD",
            "schema_version": 1,
            "payload": {"order_id": "ord-1001", "reason": "partial_discount"},
        },
        {
            "event_id": "evt-003",
            "event_type": "payment_authorized",
            "account_id": "acct-enterprise-01",
            "occurred_at_ms": base_ms + 39_000,
            "ingested_at_ms": base_ms + 42_500,
            "amount_cents": 25_000,
            "currency": "USD",
            "schema_version": 1,
            "payload": {"order_id": "ord-1001", "gateway": "stripe"},
        },
        # Second account
        {
            "event_id": "evt-004",
            "event_type": "order_created",
            "account_id": "acct-retail-02",
            "occurred_at_ms": base_ms + 18_000,
            "ingested_at_ms": base_ms + 18_050,
            "amount_cents": 12_500,
            "currency": "USD",
            "schema_version": 1,
            "payload": {"order_id": "ord-2001", "region": "us-east"},
        },
        # Advance acct-enterprise-01 watermark past 60_000 (occurred_at = 75_000 -> watermark = 65_000)
        {
            "event_id": "evt-005",
            "event_type": "order_created",
            "account_id": "acct-enterprise-01",
            "occurred_at_ms": base_ms + 75_000,
            "ingested_at_ms": base_ms + 75_080,
            "amount_cents": 40_000,
            "currency": "USD",
            "schema_version": 1,
            "payload": {"order_id": "ord-1002", "region": "us-west"},
        },
        # Excessively late event for closed window [0, 60_000) behind watermark 65_000 -> routed to DLQ
        {
            "event_id": "evt-006-late",
            "event_type": "order_created",
            "account_id": "acct-enterprise-01",
            "occurred_at_ms": base_ms + 12_000,
            "ingested_at_ms": base_ms + 76_000,
            "amount_cents": 99_900,
            "currency": "USD",
            "schema_version": 1,
            "payload": {"order_id": "ord-late"},
        },
        # Malformed schema record -> routed to DLQ
        {
            "event_id": "evt-007-bad",
            "event_type": "unsupported_event",
            "account_id": "acct-enterprise-01",
            "occurred_at_ms": base_ms + 77_000,
            "ingested_at_ms": base_ms + 77_010,
            "amount_cents": -100,
            "currency": "USD",
            "schema_version": 1,
            "payload": {},
        },
    ]


def run_demo():
    processor = StatefulStreamProcessor(
        window_size_ms=60_000, max_out_of_orderness_ms=10_000, num_partitions=8
    )
    dispositions = []
    for raw in build_demo_stream():
        status = processor.ingest(raw, observed_at_ms=raw.get("ingested_at_ms", 0))
        dispositions.append({"event_id": raw.get("event_id"), "status": status})

    processor.flush_all()

    sink = WarehouseSink(":memory:")
    sink.sink_events(processor.accepted_events)
    sink.sink_rollups(processor.closed_windows)
    sink.sink_dlq(processor.dlq)

    reconciliation = sink.verify_reconciliation(processor.stats())
    summary = {
        "dispositions": dispositions,
        "processor_stats": processor.stats(),
        "warehouse_reconciliation": reconciliation,
        "top_accounts": sink.top_accounts_summary(),
        "bigquery_ddl_preview_lines": len(bigquery_ddl("portfolio-analytics", "commerce_rt").splitlines()),
    }
    sink.close()
    return summary


if __name__ == "__main__":
    print(json.dumps(run_demo(), indent=2))

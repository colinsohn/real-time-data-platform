"""Canonical Kafka event schema validation, partition routing, and BigQuery DDL generation."""
from dataclasses import dataclass
import hashlib
import json
from typing import Any, Dict, Tuple

EVENT_TYPES = ("order_created", "payment_authorized", "shipment_updated", "refund_issued")
CURRENCIES = ("USD",)
MAX_PAYLOAD_BYTES = 64 * 1024


class SchemaValidationError(ValueError):
    """Raised when an incoming Kafka record violates the canonical event contract."""


@dataclass(frozen=True)
class StreamEvent:
    event_id: str
    event_type: str
    account_id: str
    occurred_at_ms: int
    ingested_at_ms: int
    amount_cents: int
    currency: str
    schema_version: int
    payload: Dict[str, Any]
    kafka_topic: str = "commerce.telemetry.v1"

    def signed_amount_cents(self) -> int:
        """Refunds subtract from net volume; orders add; status events are revenue-neutral."""
        if self.event_type == "refund_issued":
            return -self.amount_cents
        if self.event_type == "order_created":
            return self.amount_cents
        return 0


def deterministic_partition(account_id: str, num_partitions: int) -> int:
    """Route all events for an account_id to the same Kafka partition deterministically."""
    if num_partitions < 1:
        raise ValueError("num_partitions must be at least 1")
    if not isinstance(account_id, str) or not account_id.strip():
        raise SchemaValidationError("account_id must be a nonblank string")
    digest = hashlib.sha256(account_id.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return value % num_partitions


def validate_event(raw: Dict[str, Any], num_partitions: int = 8) -> Tuple[StreamEvent, int]:
    """Validate a raw dictionary or decoded JSON message and compute its target partition."""
    if not isinstance(raw, dict):
        raise SchemaValidationError("event record must be a JSON object")

    required = (
        "event_id",
        "event_type",
        "account_id",
        "occurred_at_ms",
        "ingested_at_ms",
        "amount_cents",
        "currency",
        "schema_version",
        "payload",
    )
    missing = [k for k in required if k not in raw]
    if missing:
        raise SchemaValidationError(f"missing required fields: {', '.join(missing)}")

    event_id = raw["event_id"]
    if not isinstance(event_id, str) or not (1 <= len(event_id.strip()) <= 128):
        raise SchemaValidationError("event_id must be a 1..128 character string")

    event_type = raw["event_type"]
    if event_type not in EVENT_TYPES:
        raise SchemaValidationError(f"unsupported event_type: {event_type}")

    account_id = raw["account_id"]
    if not isinstance(account_id, str) or not (1 <= len(account_id.strip()) <= 64):
        raise SchemaValidationError("account_id must be a 1..64 character string")

    occurred_at_ms = raw["occurred_at_ms"]
    ingested_at_ms = raw["ingested_at_ms"]
    if (
        type(occurred_at_ms) is not int
        or type(ingested_at_ms) is not int
        or occurred_at_ms <= 0
        or ingested_at_ms <= 0
    ):
        raise SchemaValidationError("occurred_at_ms and ingested_at_ms must be positive integers")

    amount_cents = raw["amount_cents"]
    if type(amount_cents) is not int or amount_cents < 0 or amount_cents > 10_000_000_00:
        raise SchemaValidationError("amount_cents must be a nonnegative integer within bounds")

    currency = raw["currency"]
    if currency not in CURRENCIES:
        raise SchemaValidationError(f"unsupported currency: {currency}")

    schema_version = raw["schema_version"]
    if type(schema_version) is not int or schema_version != 1:
        raise SchemaValidationError("schema_version must be integer 1")

    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise SchemaValidationError("payload must be a JSON object")
    encoded_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(encoded_payload.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise SchemaValidationError("payload exceeds 64 KiB limit")

    event = StreamEvent(
        event_id=event_id.strip(),
        event_type=event_type,
        account_id=account_id.strip(),
        occurred_at_ms=occurred_at_ms,
        ingested_at_ms=ingested_at_ms,
        amount_cents=amount_cents,
        currency=currency,
        schema_version=schema_version,
        payload=payload,
    )
    partition = deterministic_partition(event.account_id, num_partitions)
    return event, partition


def bigquery_ddl(project: str, dataset: str) -> str:
    """Generate production BigQuery partitioned and clustered DDL without making cloud calls."""
    if not project.strip() or not dataset.strip():
        raise ValueError("project and dataset must be nonblank")
    return f"""-- BigQuery production DDL for real-time-data-platform (offline artifact; no billable query executed)
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.commerce_events_raw` (
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,
  account_id STRING NOT NULL,
  occurred_at TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  kafka_partition INT64 NOT NULL,
  amount_cents INT64 NOT NULL,
  signed_amount_cents INT64 NOT NULL,
  currency STRING NOT NULL,
  schema_version INT64 NOT NULL,
  payload JSON NOT NULL
)
PARTITION BY DATE(occurred_at)
CLUSTER BY account_id, event_type
OPTIONS (
  require_partition_filter = TRUE,
  description = 'Deduplicated canonical commerce telemetry events from Kafka'
);

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.account_minute_rollups` (
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  account_id STRING NOT NULL,
  kafka_partition INT64 NOT NULL,
  event_count INT64 NOT NULL,
  order_count INT64 NOT NULL,
  payment_authorized_count INT64 NOT NULL,
  refund_count INT64 NOT NULL,
  gross_volume_cents INT64 NOT NULL,
  refund_volume_cents INT64 NOT NULL,
  net_volume_cents INT64 NOT NULL,
  max_event_time_ms INT64 NOT NULL,
  watermark_ms INT64 NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY account_id
OPTIONS (
  require_partition_filter = TRUE,
  description = 'Watermarked tumbling-window account revenue and event rollups'
);
"""

"""Versioned event schema contract and dead-letter validation for streaming ingestion."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Any, Dict, Mapping, Optional, Tuple, Union

SUPPORTED_SCHEMA_VERSION = "1.0"
ALLOWED_EVENT_TYPES = frozenset(
    {
        "payment_authorized",
        "payment_captured",
        "refund_issued",
        "checkout_started",
        "api_request",
    }
)


@dataclass(frozen=True)
class TelemetryEvent:
    """Validated canonical event record ready for partitioned Kafka ingestion."""

    schema_version: str
    event_id: str
    tenant_id: str
    event_type: str
    event_ts: str
    event_ts_ms: int
    event_date: str
    event_hour_utc: int
    session_id: str
    amount_cents: int
    latency_ms: float
    attributes_json: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DeadLetterRecord:
    """Quarantined invalid payload routed to the dead-letter topic (events.dlq.v1)."""

    reason_code: str
    error_detail: str
    raw_payload: str
    ingested_at_ms: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _parse_iso8601_utc(ts_str: str) -> datetime:
    if not isinstance(ts_str, str) or not ts_str.strip():
        raise ValueError("event_ts must be a non-empty ISO-8601 string")
    normalized = ts_str.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        raise ValueError("event_ts must include an explicit timezone offset or 'Z'")
    return dt.astimezone(timezone.utc)


def validate_raw_event(
    payload: Union[str, bytes, Mapping[str, Any]],
    ingested_at_ms: Optional[int] = None,
) -> Tuple[Optional[TelemetryEvent], Optional[DeadLetterRecord]]:
    """Validate a raw JSON or mapping payload against the v1.0 schema contract.

    Returns (TelemetryEvent, None) when valid, or (None, DeadLetterRecord) when rejected.
    """
    now_ms = (
        int(ingested_at_ms)
        if ingested_at_ms is not None
        else int(datetime.now(timezone.utc).timestamp() * 1000)
    )

    if isinstance(payload, (str, bytes)):
        raw_text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
        try:
            obj = json.loads(raw_text)
        except Exception as exc:
            return None, DeadLetterRecord(
                reason_code="MALFORMED_JSON",
                error_detail=str(exc),
                raw_payload=raw_text,
                ingested_at_ms=now_ms,
            )
    elif isinstance(payload, Mapping):
        obj = dict(payload)
        raw_text = json.dumps(obj, sort_keys=True, default=str)
    else:
        return None, DeadLetterRecord(
            reason_code="INVALID_PAYLOAD_TYPE",
            error_detail=f"Expected JSON object or string, got {type(payload).__name__}",
            raw_payload=str(payload),
            ingested_at_ms=now_ms,
        )

    if not isinstance(obj, Mapping):
        return None, DeadLetterRecord(
            reason_code="NON_OBJECT_JSON",
            error_detail="Top-level JSON value must be an object",
            raw_payload=raw_text,
            ingested_at_ms=now_ms,
        )

    schema_version = obj.get("schema_version", SUPPORTED_SCHEMA_VERSION)
    if schema_version != SUPPORTED_SCHEMA_VERSION:
        return None, DeadLetterRecord(
            reason_code="UNSUPPORTED_SCHEMA_VERSION",
            error_detail=f"Unsupported schema_version '{schema_version}'",
            raw_payload=raw_text,
            ingested_at_ms=now_ms,
        )

    for required_str in ("event_id", "tenant_id", "event_type", "event_ts", "session_id"):
        val = obj.get(required_str)
        if not isinstance(val, str) or not val.strip():
            return None, DeadLetterRecord(
                reason_code="MISSING_REQUIRED_FIELD",
                error_detail=f"Field '{required_str}' must be a non-empty string",
                raw_payload=raw_text,
                ingested_at_ms=now_ms,
            )

    event_type = obj["event_type"].strip()
    if event_type not in ALLOWED_EVENT_TYPES:
        return None, DeadLetterRecord(
            reason_code="INVALID_EVENT_TYPE",
            error_detail=f"Unsupported event_type '{event_type}'",
            raw_payload=raw_text,
            ingested_at_ms=now_ms,
        )

    try:
        dt_utc = _parse_iso8601_utc(obj["event_ts"])
    except Exception as exc:
        return None, DeadLetterRecord(
            reason_code="INVALID_TIMESTAMP",
            error_detail=str(exc),
            raw_payload=raw_text,
            ingested_at_ms=now_ms,
        )

    amount_raw = obj.get("amount_cents", 0)
    if isinstance(amount_raw, bool) or not isinstance(amount_raw, int) or amount_raw < 0:
        return None, DeadLetterRecord(
            reason_code="INVALID_AMOUNT_CENTS",
            error_detail="amount_cents must be a non-negative integer",
            raw_payload=raw_text,
            ingested_at_ms=now_ms,
        )

    latency_raw = obj.get("latency_ms", 0.0)
    if isinstance(latency_raw, bool) or not isinstance(latency_raw, (int, float)) or float(latency_raw) < 0.0:
        return None, DeadLetterRecord(
            reason_code="INVALID_LATENCY_MS",
            error_detail="latency_ms must be a non-negative number",
            raw_payload=raw_text,
            ingested_at_ms=now_ms,
        )

    attrs = obj.get("attributes", {})
    if attrs is None:
        attrs = {}
    if not isinstance(attrs, Mapping):
        return None, DeadLetterRecord(
            reason_code="INVALID_ATTRIBUTES",
            error_detail="attributes must be a JSON object",
            raw_payload=raw_text,
            ingested_at_ms=now_ms,
        )

    event_ts_ms = int(dt_utc.timestamp() * 1000)
    canonical_iso = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return (
        TelemetryEvent(
            schema_version=SUPPORTED_SCHEMA_VERSION,
            event_id=obj["event_id"].strip(),
            tenant_id=obj["tenant_id"].strip(),
            event_type=event_type,
            event_ts=canonical_iso,
            event_ts_ms=event_ts_ms,
            event_date=dt_utc.strftime("%Y-%m-%d"),
            event_hour_utc=dt_utc.hour,
            session_id=obj["session_id"].strip(),
            amount_cents=int(amount_raw),
            latency_ms=round(float(latency_raw), 3),
            attributes_json=json.dumps(dict(attrs), sort_keys=True),
        ),
        None,
    )

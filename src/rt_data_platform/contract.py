"""Versioned event schema contract and dead-letter validation for streaming ingestion."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, Union

SUPPORTED_SCHEMA_VERSION = "1.0"
SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0", "1.1"})
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
class FieldSpec:
    """Typed field specification inside a versioned schema contract."""

    name: str
    field_type: str
    required: bool
    has_default: bool = False


@dataclass(frozen=True)
class SchemaDefinition:
    """Versioned event schema definition for compatibility validation."""

    version: str
    fields: Tuple[FieldSpec, ...]

    def field_map(self) -> Dict[str, FieldSpec]:
        return {f.name: f for f in self.fields}


class SchemaRegistry:
    """In-memory schema evolution registry enforcing BACKWARD and FULL compatibility."""

    def __init__(self) -> None:
        self._schemas: Dict[str, SchemaDefinition] = {}
        self._order: List[str] = []
        self.register(
            SchemaDefinition(
                version="1.0",
                fields=(
                    FieldSpec("event_id", "string", required=True),
                    FieldSpec("tenant_id", "string", required=True),
                    FieldSpec("event_type", "string", required=True),
                    FieldSpec("event_ts", "timestamp", required=True),
                    FieldSpec("session_id", "string", required=True),
                    FieldSpec("amount_cents", "int64", required=False, has_default=True),
                    FieldSpec("latency_ms", "float64", required=False, has_default=True),
                    FieldSpec("attributes", "json_object", required=False, has_default=True),
                ),
            ),
            compatibility="NONE",
        )
        self.register(
            SchemaDefinition(
                version="1.1",
                fields=(
                    FieldSpec("event_id", "string", required=True),
                    FieldSpec("tenant_id", "string", required=True),
                    FieldSpec("event_type", "string", required=True),
                    FieldSpec("event_ts", "timestamp", required=True),
                    FieldSpec("session_id", "string", required=True),
                    FieldSpec("amount_cents", "int64", required=False, has_default=True),
                    FieldSpec("latency_ms", "float64", required=False, has_default=True),
                    FieldSpec("attributes", "json_object", required=False, has_default=True),
                    FieldSpec("region", "string", required=False, has_default=True),
                ),
            ),
            compatibility="FULL",
        )

    def check_compatibility(
        self, candidate: SchemaDefinition, mode: str = "BACKWARD"
    ) -> Tuple[bool, List[str]]:
        if not self._order or mode == "NONE":
            return True, []
        prev = self._schemas[self._order[-1]]
        prev_map = prev.field_map()
        cand_map = candidate.field_map()
        errors: List[str] = []

        # Type stability across shared fields
        for name, p_field in prev_map.items():
            if name in cand_map and cand_map[name].field_type != p_field.field_type:
                errors.append(
                    f"Field '{name}' changed type from '{p_field.field_type}' to '{cand_map[name].field_type}'"
                )

        if mode in ("BACKWARD", "FULL"):
            # New fields added in candidate must not be required without a default (so v_prev payloads remain readable)
            for name, c_field in cand_map.items():
                if name not in prev_map and (c_field.required and not c_field.has_default):
                    errors.append(
                        f"BACKWARD incompatibility: new field '{name}' is required without a default"
                    )

        if mode in ("FORWARD", "FULL"):
            # Fields removed from prev must not have been required without a default
            for name, p_field in prev_map.items():
                if name not in cand_map and (p_field.required and not p_field.has_default):
                    errors.append(
                        f"FORWARD incompatibility: removed required field '{name}' without default"
                    )

        return len(errors) == 0, errors

    def register(self, schema: SchemaDefinition, compatibility: str = "BACKWARD") -> None:
        ok, errs = self.check_compatibility(schema, mode=compatibility)
        if not ok:
            raise ValueError("; ".join(errs))
        self._schemas[schema.version] = schema
        if schema.version not in self._order:
            self._order.append(schema.version)

    def is_supported(self, version: str) -> bool:
        return version in self._schemas

    def versions(self) -> Tuple[str, ...]:
        return tuple(self._order)


DEFAULT_SCHEMA_REGISTRY = SchemaRegistry()


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
    quarantine_id: str = ""
    schema_version_attempted: str = "unknown"
    replay_eligible: bool = False

    def __post_init__(self) -> None:
        if not self.quarantine_id:
            digest = hashlib.sha256(
                f"{self.reason_code}:{self.ingested_at_ms}:{self.raw_payload}".encode("utf-8")
            ).hexdigest()[:16]
            object.__setattr__(self, "quarantine_id", f"dlq-{digest}")
        if self.reason_code in {
            "MISSING_REQUIRED_FIELD",
            "INVALID_TIMESTAMP",
            "INVALID_AMOUNT_CENTS",
            "INVALID_LATENCY_MS",
            "INVALID_ATTRIBUTES",
        }:
            object.__setattr__(self, "replay_eligible", True)

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

    schema_version = str(obj.get("schema_version", SUPPORTED_SCHEMA_VERSION))
    if not DEFAULT_SCHEMA_REGISTRY.is_supported(schema_version):
        return None, DeadLetterRecord(
            reason_code="UNSUPPORTED_SCHEMA_VERSION",
            error_detail=f"Unsupported schema_version '{schema_version}'",
            raw_payload=raw_text,
            ingested_at_ms=now_ms,
            schema_version_attempted=schema_version,
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

    attrs_dict = dict(attrs)
    if schema_version == "1.1" and "region" in obj and isinstance(obj["region"], str) and obj["region"].strip():
        attrs_dict.setdefault("region", obj["region"].strip())

    event_ts_ms = int(dt_utc.timestamp() * 1000)
    canonical_iso = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return (
        TelemetryEvent(
            schema_version=schema_version,
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
            attributes_json=json.dumps(attrs_dict, sort_keys=True),
        ),
        None,
    )

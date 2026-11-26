"""Stateful stream processor with event-ID deduplication, watermarks, tumbling windows, and DLQ."""
from collections import OrderedDict
from dataclasses import dataclass
import json
from typing import Any, Dict, List, Tuple

from platform_core.schema import SchemaValidationError, StreamEvent, validate_event


@dataclass(frozen=True)
class WindowAggregate:
    window_start_ms: int
    window_end_ms: int
    account_id: str
    kafka_partition: int
    event_count: int
    order_count: int
    payment_authorized_count: int
    refund_count: int
    gross_volume_cents: int
    refund_volume_cents: int
    net_volume_cents: int
    max_event_time_ms: int
    watermark_ms: int


@dataclass(frozen=True)
class DeadLetterRecord:
    raw_payload: str
    reason: str
    category: str  # "schema_invalid" or "late_arrival"
    observed_at_ms: int


class _MutableWindow:
    def __init__(self, window_start_ms: int, window_end_ms: int, account_id: str, partition: int):
        self.window_start_ms = window_start_ms
        self.window_end_ms = window_end_ms
        self.account_id = account_id
        self.kafka_partition = partition
        self.event_count = 0
        self.order_count = 0
        self.payment_authorized_count = 0
        self.refund_count = 0
        self.gross_volume_cents = 0
        self.refund_volume_cents = 0
        self.net_volume_cents = 0
        self.max_event_time_ms = window_start_ms

    def apply(self, event: StreamEvent) -> None:
        self.event_count += 1
        if event.event_type == "order_created":
            self.order_count += 1
            self.gross_volume_cents += event.amount_cents
            self.net_volume_cents += event.amount_cents
        elif event.event_type == "payment_authorized":
            self.payment_authorized_count += 1
        elif event.event_type == "refund_issued":
            self.refund_count += 1
            self.refund_volume_cents += event.amount_cents
            self.net_volume_cents -= event.amount_cents
        if event.occurred_at_ms > self.max_event_time_ms:
            self.max_event_time_ms = event.occurred_at_ms

    def freeze(self, watermark_ms: int) -> WindowAggregate:
        return WindowAggregate(
            window_start_ms=self.window_start_ms,
            window_end_ms=self.window_end_ms,
            account_id=self.account_id,
            kafka_partition=self.kafka_partition,
            event_count=self.event_count,
            order_count=self.order_count,
            payment_authorized_count=self.payment_authorized_count,
            refund_count=self.refund_count,
            gross_volume_cents=self.gross_volume_cents,
            refund_volume_cents=self.refund_volume_cents,
            net_volume_cents=self.net_volume_cents,
            max_event_time_ms=self.max_event_time_ms,
            watermark_ms=watermark_ms,
        )


class StatefulStreamProcessor:
    """Single-node deterministic stream processor modeling partitioned Kafka consumers."""

    def __init__(
        self,
        window_size_ms: int = 60_000,
        max_out_of_orderness_ms: int = 10_000,
        num_partitions: int = 8,
        dedup_capacity: int = 200_000,
    ):
        if window_size_ms < 1000 or max_out_of_orderness_ms < 0 or num_partitions < 1 or dedup_capacity < 1:
            raise ValueError("invalid processor parameters")
        self.window_size_ms = window_size_ms
        self.max_out_of_orderness_ms = max_out_of_orderness_ms
        self.num_partitions = num_partitions
        self.dedup_capacity = dedup_capacity

        self._seen_ids: OrderedDict[str, int] = OrderedDict()
        self._max_event_time_by_partition: Dict[int, int] = {}
        self._open_windows: Dict[Tuple[int, int, str], _MutableWindow] = {}
        self.accepted_events: List[Tuple[StreamEvent, int]] = []
        self.closed_windows: List[WindowAggregate] = []
        self.dlq: List[DeadLetterRecord] = []

        self.total_received = 0
        self.duplicates_dropped = 0
        self.late_events_routed_to_dlq = 0
        self.invalid_events_routed_to_dlq = 0

    def partition_watermark(self, partition: int) -> int:
        max_ts = self._max_event_time_by_partition.get(partition)
        if max_ts is None:
            return 0
        return max(0, max_ts - self.max_out_of_orderness_ms)

    def ingest(self, raw_record: Dict[str, Any], observed_at_ms: int = 0) -> str:
        """Process one incoming Kafka message and return its disposition status."""
        self.total_received += 1
        try:
            event, partition = validate_event(raw_record, self.num_partitions)
        except SchemaValidationError as exc:
            self.invalid_events_routed_to_dlq += 1
            self.dlq.append(
                DeadLetterRecord(
                    raw_payload=json.dumps(raw_record, sort_keys=True, default=str),
                    reason=str(exc),
                    category="schema_invalid",
                    observed_at_ms=observed_at_ms,
                )
            )
            return "invalid"

        # Exactly-once event_id deduplication within bounded LRU window.
        if event.event_id in self._seen_ids:
            self.duplicates_dropped += 1
            self._seen_ids.move_to_end(event.event_id)
            return "duplicate"

        window_start = (event.occurred_at_ms // self.window_size_ms) * self.window_size_ms
        window_end = window_start + self.window_size_ms
        current_wm = self.partition_watermark(partition)

        # If the partition watermark has already passed window_end, the window is closed.
        if current_wm >= window_end:
            self.late_events_routed_to_dlq += 1
            self.dlq.append(
                DeadLetterRecord(
                    raw_payload=json.dumps(raw_record, sort_keys=True),
                    reason=f"event_time {event.occurred_at_ms} belongs to closed window [{window_start}, {window_end}) behind watermark {current_wm}",
                    category="late_arrival",
                    observed_at_ms=event.ingested_at_ms,
                )
            )
            return "late"

        self._seen_ids[event.event_id] = event.occurred_at_ms
        if len(self._seen_ids) > self.dedup_capacity:
            self._seen_ids.popitem(last=False)

        prev_max = self._max_event_time_by_partition.get(partition, 0)
        watermark_advanced = False
        if event.occurred_at_ms > prev_max:
            old_wm_bucket = self.partition_watermark(partition) // self.window_size_ms
            self._max_event_time_by_partition[partition] = event.occurred_at_ms
            new_wm_bucket = self.partition_watermark(partition) // self.window_size_ms
            watermark_advanced = new_wm_bucket > old_wm_bucket

        key = (partition, window_start, event.account_id)
        win = self._open_windows.get(key)
        if win is None:
            win = _MutableWindow(window_start, window_end, event.account_id, partition)
            self._open_windows[key] = win
        win.apply(event)
        self.accepted_events.append((event, partition))

        if watermark_advanced:
            self._emit_closed_windows_for_partition(partition)
        return "accepted"

    def _emit_closed_windows_for_partition(self, partition: int) -> None:
        wm = self.partition_watermark(partition)
        to_close = [
            key
            for key, win in self._open_windows.items()
            if key[0] == partition and wm >= win.window_end_ms
        ]
        to_close.sort()
        for key in to_close:
            win = self._open_windows.pop(key)
            self.closed_windows.append(win.freeze(wm))

    def flush_all(self) -> List[WindowAggregate]:
        """Advance all partition watermarks to infinity on clean shutdown and emit open windows."""
        remaining_keys = sorted(self._open_windows.keys())
        newly_closed = []
        for key in remaining_keys:
            win = self._open_windows.pop(key)
            wm = max(self.partition_watermark(win.kafka_partition), win.window_end_ms)
            frozen = win.freeze(wm)
            self.closed_windows.append(frozen)
            newly_closed.append(frozen)
        return newly_closed

    def stats(self) -> Dict[str, int]:
        return {
            "total_received": self.total_received,
            "accepted_unique": len(self.accepted_events),
            "duplicates_dropped": self.duplicates_dropped,
            "late_events_dlq": self.late_events_routed_to_dlq,
            "invalid_events_dlq": self.invalid_events_routed_to_dlq,
            "open_windows": len(self._open_windows),
            "closed_windows": len(self.closed_windows),
            "total_dlq": len(self.dlq),
        }

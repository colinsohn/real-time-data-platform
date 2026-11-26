"""End-to-end streaming pipeline coordinating schema validation, partitioned Kafka topics,
dead-letter quarantine, SQL warehouse deduplication, and atomic offset commits."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Union

from .contract import validate_raw_event
from .kafka_log import KafkaRecord, PartitionedTopicLog
from .warehouse import BatchLoadResult, BigQueryCompiler, SQLWarehouseLoader


@dataclass(frozen=True)
class IngestionSummary:
    """Result of validating and producing raw payloads into primary and DLQ topics."""

    total_submitted: int
    valid_produced: int
    dead_letter_produced: int


class StreamingPipeline:
    """Coordinates producer validation, partitioned Kafka storage, and two-phase SQL warehouse commits."""

    def __init__(
        self,
        num_partitions: int = 8,
        storage_dir: Optional[Path] = None,
        db_path: Optional[Path] = None,
        consumer_group: str = "cg-warehouse-loader-v1",
        gcp_project: str = "portfolio-analytics-prod",
        bq_dataset: str = "realtime_telemetry",
    ) -> None:
        self.num_partitions = num_partitions
        self.consumer_group = consumer_group
        self.raw_topic = PartitionedTopicLog(
            topic="events.raw.v1",
            num_partitions=num_partitions,
            storage_dir=storage_dir,
        )
        self.dlq_topic = PartitionedTopicLog(
            topic="events.dlq.v1",
            num_partitions=max(1, num_partitions // 2),
            storage_dir=storage_dir,
        )
        self.warehouse = SQLWarehouseLoader(db_path=db_path, prefer_duckdb=True)
        self.bq_compiler = BigQueryCompiler(project_id=gcp_project, dataset_id=bq_dataset)
        self._batch_seq = 0

    def ingest_raw_events(
        self,
        payloads: Sequence[Union[str, bytes, Mapping[str, Any]]],
        ingested_at_ms: Optional[int] = None,
    ) -> IngestionSummary:
        """Validate raw event payloads and route them to events.raw.v1 or events.dlq.v1."""
        valid_count = 0
        dlq_count = 0
        for item in payloads:
            event, dlq = validate_raw_event(item, ingested_at_ms=ingested_at_ms)
            if event is not None:
                self.raw_topic.produce(
                    key=event.tenant_id,
                    value=event.to_dict(),
                    timestamp_ms=event.event_ts_ms,
                )
                valid_count += 1
            elif dlq is not None:
                self.dlq_topic.produce(
                    key=dlq.reason_code,
                    value=dlq.to_dict(),
                    timestamp_ms=dlq.ingested_at_ms,
                )
                dlq_count += 1
        return IngestionSummary(
            total_submitted=len(payloads),
            valid_produced=valid_count,
            dead_letter_produced=dlq_count,
        )

    def process_next_micro_batch(
        self,
        max_records: int = 500,
        commit_offsets: bool = True,
    ) -> BatchLoadResult:
        """Poll uncommitted records, deduplicate & load into the SQL warehouse, and commit offsets.

        If commit_offsets=False (simulating a worker crash after warehouse commit but before
        Kafka offset commit), a subsequent call will re-poll the same Kafka records and the
        warehouse's idempotent event_id deduplication will drop all duplicates cleanly.
        """
        self._batch_seq += 1
        batch_id = f"batch-{self._batch_seq:05d}"
        records: Sequence[KafkaRecord] = self.raw_topic.poll_uncommitted(
            consumer_group=self.consumer_group,
            max_records=max_records,
        )
        result = self.warehouse.load_micro_batch(batch_id=batch_id, records=records)
        if commit_offsets and result.next_offsets:
            self.raw_topic.commit_offsets(
                consumer_group=self.consumer_group,
                next_offsets=result.next_offsets,
            )
        return result

    def diagnostics(self) -> Dict[str, Any]:
        """Return end-to-end topic watermarks, consumer lag, DLQ depth, and warehouse totals."""
        dlq_watermarks = self.dlq_topic.high_watermarks()
        return {
            "sql_engine": self.warehouse.engine_name,
            "num_partitions": self.num_partitions,
            "consumer_group": self.consumer_group,
            "raw_topic_watermarks": self.raw_topic.high_watermarks(),
            "raw_topic_total_records": sum(self.raw_topic.high_watermarks().values()),
            "consumer_lag_by_partition": self.raw_topic.consumer_lag(self.consumer_group),
            "consumer_total_lag": sum(self.raw_topic.consumer_lag(self.consumer_group).values()),
            "dlq_total_records": sum(dlq_watermarks.values()),
            "warehouse_unique_events": self.warehouse.core_event_count(),
            "warehouse_total_amount_cents": self.warehouse.total_amount_cents(),
        }

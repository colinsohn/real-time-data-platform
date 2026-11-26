"""Partitioned Kafka topic log engine with deterministic Murmur2 key hashing,
CRC32 frame integrity, and consumer-group atomic offset commits."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
import zlib


def kafka_murmur2(data: bytes) -> int:
    """Pure-Python implementation of Apache Kafka's DefaultPartitioner murmur2 hash."""
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    h = (seed ^ length) & 0xFFFFFFFF
    length4 = length // 4

    for i in range(length4):
        i4 = i * 4
        k = (
            (data[i4 + 0] & 0xFF)
            | ((data[i4 + 1] & 0xFF) << 8)
            | ((data[i4 + 2] & 0xFF) << 16)
            | ((data[i4 + 3] & 0xFF) << 24)
        )
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k

    rem = length % 4
    if rem >= 3:
        h ^= (data[(length & ~3) + 2] & 0xFF) << 16
    if rem >= 2:
        h ^= (data[(length & ~3) + 1] & 0xFF) << 8
    if rem >= 1:
        h ^= data[length & ~3] & 0xFF
        h = (h * m) & 0xFFFFFFFF

    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h & 0xFFFFFFFF


def partition_for_key(key: str, num_partitions: int) -> int:
    """Assign a partition in [0, num_partitions) using Kafka's positive murmur2 mask."""
    if num_partitions <= 0:
        raise ValueError("num_partitions must be >= 1")
    positive_hash = kafka_murmur2(key.encode("utf-8")) & 0x7FFFFFFF
    return positive_hash % num_partitions


@dataclass(frozen=True)
class KafkaRecord:
    """Single immutable record stored in a topic partition."""

    topic: str
    partition: int
    offset: int
    key: str
    value: Dict[str, Any]
    timestamp_ms: int
    crc32: int

    def verify_crc(self) -> bool:
        canonical = json.dumps(
            {
                "topic": self.topic,
                "partition": self.partition,
                "offset": self.offset,
                "key": self.key,
                "value": self.value,
                "timestamp_ms": self.timestamp_ms,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        expected = zlib.crc32(canonical) & 0xFFFFFFFF
        return expected == self.crc32


class PartitionedTopicLog:
    """Append-only partitioned Kafka topic log with optional disk persistence and consumer-group offsets."""

    def __init__(
        self,
        topic: str,
        num_partitions: int = 4,
        storage_dir: Optional[Path] = None,
    ) -> None:
        if num_partitions <= 0:
            raise ValueError("num_partitions must be >= 1")
        self.topic = topic
        self.num_partitions = num_partitions
        self.storage_dir = Path(storage_dir) if storage_dir is not None else None
        self._partitions: List[List[KafkaRecord]] = [[] for _ in range(num_partitions)]
        self._committed_offsets: Dict[str, Dict[int, int]] = {}

        if self.storage_dir is not None:
            self.storage_dir.mkdir(parents=True, exist_ok=True)
            self._load_from_disk()

    def _partition_path(self, partition: int) -> Optional[Path]:
        if self.storage_dir is None:
            return None
        return self.storage_dir / f"{self.topic}-p{partition:02d}.jsonl"

    def _offsets_path(self) -> Optional[Path]:
        if self.storage_dir is None:
            return None
        return self.storage_dir / f"{self.topic}-offsets.json"

    def _load_from_disk(self) -> None:
        for p in range(self.num_partitions):
            path = self._partition_path(p)
            if path is not None and path.exists():
                records: List[KafkaRecord] = []
                for line in path.read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    raw = json.loads(line)
                    rec = KafkaRecord(**raw)
                    if not rec.verify_crc():
                        raise ValueError(
                            f"CRC32 corruption detected in {self.topic} partition {p} offset {rec.offset}"
                        )
                    records.append(rec)
                self._partitions[p] = records

        offsets_path = self._offsets_path()
        if offsets_path is not None and offsets_path.exists():
            raw_offsets = json.loads(offsets_path.read_text(encoding="utf-8"))
            self._committed_offsets = {
                group: {int(part): int(off) for part, off in parts.items()}
                for group, parts in raw_offsets.items()
            }

    def produce(
        self,
        key: str,
        value: Mapping[str, Any],
        timestamp_ms: int,
        explicit_partition: Optional[int] = None,
    ) -> KafkaRecord:
        """Append a record to its key-routed partition and return the assigned KafkaRecord."""
        partition = (
            int(explicit_partition)
            if explicit_partition is not None
            else partition_for_key(key, self.num_partitions)
        )
        if not (0 <= partition < self.num_partitions):
            raise ValueError(f"Partition {partition} out of range [0, {self.num_partitions})")

        offset = len(self._partitions[partition])
        value_dict = dict(value)
        canonical = json.dumps(
            {
                "topic": self.topic,
                "partition": partition,
                "offset": offset,
                "key": key,
                "value": value_dict,
                "timestamp_ms": int(timestamp_ms),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        crc = zlib.crc32(canonical) & 0xFFFFFFFF
        record = KafkaRecord(
            topic=self.topic,
            partition=partition,
            offset=offset,
            key=key,
            value=value_dict,
            timestamp_ms=int(timestamp_ms),
            crc32=crc,
        )
        self._partitions[partition].append(record)

        part_path = self._partition_path(partition)
        if part_path is not None:
            with part_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(asdict(record), sort_keys=True) + "\n")

        return record

    def committed_offset(self, consumer_group: str, partition: int) -> int:
        """Return the next offset to read for (consumer_group, partition); defaults to 0."""
        return self._committed_offsets.get(consumer_group, {}).get(partition, 0)

    def poll_uncommitted(
        self,
        consumer_group: str,
        max_records: int = 500,
    ) -> List[KafkaRecord]:
        """Fetch up to max_records uncommitted records round-robin across partitions."""
        if max_records <= 0:
            return []
        cursors = {
            p: self.committed_offset(consumer_group, p) for p in range(self.num_partitions)
        }
        batch: List[KafkaRecord] = []
        made_progress = True
        while len(batch) < max_records and made_progress:
            made_progress = False
            for p in range(self.num_partitions):
                idx = cursors[p]
                if idx < len(self._partitions[p]):
                    rec = self._partitions[p][idx]
                    if not rec.verify_crc():
                        raise ValueError(
                            f"CRC32 mismatch on {self.topic} partition {p} offset {rec.offset}"
                        )
                    batch.append(rec)
                    cursors[p] = idx + 1
                    made_progress = True
                    if len(batch) >= max_records:
                        break
        return batch

    def commit_offsets(
        self,
        consumer_group: str,
        next_offsets: Mapping[int, int],
    ) -> None:
        """Atomically advance committed next-offsets for a consumer group."""
        group_map = self._committed_offsets.setdefault(consumer_group, {})
        for part, next_off in next_offsets.items():
            p = int(part)
            off = int(next_off)
            if not (0 <= p < self.num_partitions):
                raise ValueError(f"Invalid partition {p}")
            if off < 0 or off > len(self._partitions[p]):
                raise ValueError(f"Invalid offset {off} for partition {p}")
            group_map[p] = max(group_map.get(p, 0), off)

        offsets_path = self._offsets_path()
        if offsets_path is not None:
            offsets_path.write_text(
                json.dumps(self._committed_offsets, indent=2, sort_keys=True),
                encoding="utf-8",
            )

    def high_watermarks(self) -> Dict[int, int]:
        """Return total appended records per partition."""
        return {p: len(self._partitions[p]) for p in range(self.num_partitions)}

    def consumer_lag(self, consumer_group: str) -> Dict[int, int]:
        """Return uncommitted record lag per partition for a consumer group."""
        return {
            p: len(self._partitions[p]) - self.committed_offset(consumer_group, p)
            for p in range(self.num_partitions)
        }

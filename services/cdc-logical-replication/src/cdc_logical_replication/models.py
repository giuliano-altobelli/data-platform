from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    """Single wal2json event destined for Kinesis."""

    lsn: int
    payload: bytes
    partition_key: str

    @property
    def record_size_bytes(self) -> int:
        # Kinesis request sizing is dominated by payload bytes plus partition key length.
        return len(self.payload) + len(self.partition_key.encode("utf-8"))

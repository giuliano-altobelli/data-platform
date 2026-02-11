from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Sequence
from typing import Any, Protocol

from cdc_logical_replication.ack import AckTracker
from cdc_logical_replication.batching import MicroBatcher
from cdc_logical_replication.models import ChangeEvent
from cdc_logical_replication.queue import InflightEventQueue

LOGGER = logging.getLogger(__name__)

try:
    import boto3
except ImportError:  # pragma: no cover - only used at runtime in deployed container.
    boto3 = None


class KinesisClient(Protocol):
    def put_records(self, *, StreamName: str, Records: list[dict[str, Any]]) -> dict[str, Any]:
        ...


def create_kinesis_client(*, region_name: str) -> KinesisClient:
    if boto3 is None:
        raise RuntimeError("boto3 is required to create a Kinesis client")

    return boto3.client("kinesis", region_name=region_name)


class KinesisPublisher:
    def __init__(
        self,
        *,
        client: KinesisClient,
        stream_name: str,
        max_records: int,
        max_bytes: int,
        max_delay_ms: int,
        retry_base_delay_ms: int,
        retry_max_delay_ms: int,
    ) -> None:
        self._client = client
        self._stream_name = stream_name
        self._retry_base_s = retry_base_delay_ms / 1000.0
        self._retry_max_s = retry_max_delay_ms / 1000.0
        self._batcher = MicroBatcher(
            max_records=max_records,
            max_bytes=max_bytes,
            max_delay_ms=max_delay_ms,
        )

    async def run(
        self,
        *,
        queue: InflightEventQueue,
        ack_tracker: AckTracker,
        frontier_updates: asyncio.Queue[int],
    ) -> None:
        while True:
            batch = await self._batcher.next_batch(queue)
            if not batch:
                continue

            await self._publish_with_retries(
                batch=batch,
                queue=queue,
                ack_tracker=ack_tracker,
                frontier_updates=frontier_updates,
            )

    async def _publish_with_retries(
        self,
        *,
        batch: Sequence[ChangeEvent],
        queue: InflightEventQueue,
        ack_tracker: AckTracker,
        frontier_updates: asyncio.Queue[int],
    ) -> None:
        pending: list[ChangeEvent] = list(batch)
        attempt = 0

        while pending:
            try:
                response = await self._put_records(pending)
                returned = response.get("Records", [])
                if len(returned) != len(pending):
                    raise RuntimeError(
                        "PutRecords returned mismatched result count "
                        f"({len(returned)} != {len(pending)})"
                    )
            except Exception:
                LOGGER.exception(
                    "kinesis_put_records_failed",
                    extra={
                        "pending_count": len(pending),
                        "attempt": attempt,
                    },
                )
                await asyncio.sleep(self._retry_delay(attempt))
                attempt += 1
                continue

            failed: list[ChangeEvent] = []
            for event, result in zip(pending, returned, strict=True):
                if result.get("ErrorCode"):
                    failed.append(event)
                    continue

                advanced_to = ack_tracker.mark_published(event.lsn)
                if advanced_to is not None:
                    frontier_updates.put_nowait(advanced_to)
                await queue.task_done(event)

            if failed:
                LOGGER.warning(
                    "kinesis_partial_failure",
                    extra={
                        "failed": len(failed),
                        "succeeded": len(pending) - len(failed),
                        "attempt": attempt,
                    },
                )
                await asyncio.sleep(self._retry_delay(attempt))
                attempt += 1

            pending = failed

    async def _put_records(self, events: Sequence[ChangeEvent]) -> dict[str, Any]:
        payload = [{"Data": e.payload, "PartitionKey": e.partition_key} for e in events]
        return await asyncio.to_thread(
            self._client.put_records,
            StreamName=self._stream_name,
            Records=payload,
        )

    def _retry_delay(self, attempt: int) -> float:
        exponential = min(self._retry_max_s, self._retry_base_s * (2**attempt))
        return exponential * random.uniform(0.8, 1.2)

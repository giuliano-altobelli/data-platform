from __future__ import annotations

import asyncio

from cdc_logical_replication.models import ChangeEvent
from cdc_logical_replication.queue import InflightEventQueue


def _event(lsn: int, payload_size: int) -> ChangeEvent:
    return ChangeEvent(lsn=lsn, payload=(b"x" * payload_size), partition_key="pk")


def test_queue_backpressure_blocks_when_message_capacity_reached() -> None:
    async def scenario() -> None:
        queue = InflightEventQueue(max_messages=1, max_bytes=1000)
        first = _event(1, 10)
        second = _event(2, 10)

        await queue.put(first)

        blocked_put = asyncio.create_task(queue.put(second))
        await asyncio.sleep(0.05)
        assert not blocked_put.done()

        popped = await queue.get()
        await queue.task_done(popped)

        await asyncio.wait_for(blocked_put, timeout=1.0)
        assert queue.qsize() == 1

    asyncio.run(scenario())


def test_queue_backpressure_blocks_when_byte_capacity_reached() -> None:
    async def scenario() -> None:
        queue = InflightEventQueue(max_messages=10, max_bytes=12)
        first = _event(1, 10)  # payload 10 + key 2 => 12
        second = _event(2, 1)  # payload 1 + key 2 => 3

        await queue.put(first)
        blocked_put = asyncio.create_task(queue.put(second))
        await asyncio.sleep(0.05)
        assert not blocked_put.done()

        popped = await queue.get()
        await queue.task_done(popped)

        await asyncio.wait_for(blocked_put, timeout=1.0)
        assert queue.bytes_inflight == second.record_size_bytes

    asyncio.run(scenario())

from __future__ import annotations

import asyncio
from time import monotonic
from typing import Any

import psycopg

from src.cdc_logical_replication.ack import AckTracker
from src.cdc_logical_replication.models import ChangeEvent
from src.cdc_logical_replication.partition_key import extract_partition_key
from src.cdc_logical_replication.protocol import (
    ReplicationProtocolError,
    build_standby_status,
    lsn_int_to_str,
    parse_keepalive,
    parse_xlogdata,
)
from src.cdc_logical_replication.queue import InflightEventQueue
from src.cdc_logical_replication.settings import Settings


def build_start_replication_statement(
    *,
    slot_name: str,
    start_lsn: int,
    wal2json_options_sql: str,
) -> str:
    return (
        f"START_REPLICATION SLOT {slot_name} "
        f"LOGICAL {lsn_int_to_str(start_lsn)} "
        f"({wal2json_options_sql})"
    )


async def consume_replication_stream(
    *,
    settings: Settings,
    queue: InflightEventQueue,
    ack_tracker: AckTracker,
    frontier_updates: asyncio.Queue[int],
) -> None:
    connection = await psycopg.AsyncConnection.connect(
        conninfo=settings.postgres_conninfo,
        autocommit=True,
        replication="database",
    )

    try:
        async with connection.cursor() as cursor:
            statement = build_start_replication_statement(
                slot_name=settings.replication_slot,
                start_lsn=ack_tracker.frontier_lsn,
                wal2json_options_sql=settings.wal2json_options_sql,
            )

            async with cursor.copy(statement) as copy:
                await _replication_loop(
                    copy=copy,
                    settings=settings,
                    queue=queue,
                    ack_tracker=ack_tracker,
                    frontier_updates=frontier_updates,
                )
    finally:
        await connection.close()


async def _replication_loop(
    *,
    copy: Any,
    settings: Settings,
    queue: InflightEventQueue,
    ack_tracker: AckTracker,
    frontier_updates: asyncio.Queue[int],
) -> None:
    latest_safe_lsn = ack_tracker.frontier_lsn
    last_feedback_lsn = latest_safe_lsn
    last_feedback_at = monotonic()

    while True:
        latest_safe_lsn = _drain_frontier_updates(frontier_updates, default=latest_safe_lsn)
        if latest_safe_lsn > last_feedback_lsn:
            await copy.write(build_standby_status(latest_safe_lsn, reply_requested=0))
            last_feedback_lsn = latest_safe_lsn
            last_feedback_at = monotonic()

        elapsed = monotonic() - last_feedback_at
        timeout = max(0.01, settings.replication_feedback_interval_s - elapsed)

        try:
            raw_frame = await asyncio.wait_for(copy.read(), timeout=timeout)
        except asyncio.TimeoutError:
            await copy.write(build_standby_status(latest_safe_lsn, reply_requested=0))
            last_feedback_lsn = latest_safe_lsn
            last_feedback_at = monotonic()
            continue

        if raw_frame is None:
            continue

        frame = bytes(raw_frame)
        if not frame:
            continue

        tag = frame[:1]
        if tag == b"w":
            _, wal_end, _, payload = parse_xlogdata(frame)
            partition_key = extract_partition_key(
                payload,
                lsn=wal_end,
                mode=settings.partition_key_mode,
                fallback=settings.partition_key_fallback,
                static_fallback_value=settings.partition_key_static_value,
            )
            ack_tracker.register(wal_end)
            await queue.put(ChangeEvent(lsn=wal_end, payload=payload, partition_key=partition_key))
            continue

        if tag == b"k":
            _, _, reply_requested = parse_keepalive(frame)
            if reply_requested:
                latest_safe_lsn = _drain_frontier_updates(frontier_updates, default=latest_safe_lsn)
                await copy.write(build_standby_status(latest_safe_lsn, reply_requested=1))
                last_feedback_lsn = latest_safe_lsn
                last_feedback_at = monotonic()
            continue

        raise ReplicationProtocolError(f"Unknown replication frame tag: {tag!r}")


def _drain_frontier_updates(frontier_updates: asyncio.Queue[int], *, default: int) -> int:
    newest = default
    while True:
        try:
            newest = frontier_updates.get_nowait()
        except asyncio.QueueEmpty:
            return newest

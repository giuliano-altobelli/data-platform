from __future__ import annotations

import asyncio
from typing import Any

import psycopg
from pydantic import BaseModel, ConfigDict


class LeaderSession(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    connection: psycopg.AsyncConnection[Any]
    lock_key: int

    async def close(self) -> None:
        await self.connection.close()


async def try_acquire_leader_lock(*, conninfo: str, lock_key: int) -> LeaderSession | None:
    connection = await psycopg.AsyncConnection.connect(conninfo=conninfo, autocommit=True)
    try:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
            row = await cursor.fetchone()
    except Exception:
        await connection.close()
        raise

    if not row or row[0] is not True:
        await connection.close()
        return None

    return LeaderSession(connection=connection, lock_key=lock_key)


async def wait_for_leadership(
    *,
    conninfo: str,
    lock_key: int,
    retry_interval_s: float,
) -> LeaderSession:
    while True:
        session = await try_acquire_leader_lock(conninfo=conninfo, lock_key=lock_key)
        if session is not None:
            return session
        await asyncio.sleep(retry_interval_s)


async def leadership_watchdog(
    session: LeaderSession,
    *,
    interval_s: float,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(interval_s)
        async with session.connection.cursor() as cursor:
            await cursor.execute("SELECT 1")
            _ = await cursor.fetchone()

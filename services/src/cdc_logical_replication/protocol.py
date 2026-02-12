from __future__ import annotations

import struct
import time

XLOGDATA_HDR = struct.Struct("!cqqq")
KEEPALIVE = struct.Struct("!cqqB")
STANDBY_STATUS = struct.Struct("!cqqqqB")


class ReplicationProtocolError(RuntimeError):
    """Raised when replication protocol payloads are invalid."""


def now_us() -> int:
    return int(time.time() * 1_000_000)


def parse_xlogdata(buf: bytes) -> tuple[int, int, int, bytes]:
    if len(buf) < XLOGDATA_HDR.size:
        raise ReplicationProtocolError("XLogData frame is too short")

    tag, wal_start, wal_end, server_time_us = XLOGDATA_HDR.unpack_from(buf, 0)
    if tag != b"w":
        raise ReplicationProtocolError(f"Expected XLogData tag b'w', got {tag!r}")

    payload = bytes(buf[XLOGDATA_HDR.size:])
    return wal_start, wal_end, server_time_us, payload


def parse_keepalive(buf: bytes) -> tuple[int, int, int]:
    if len(buf) != KEEPALIVE.size:
        raise ReplicationProtocolError("Keepalive frame has invalid size")

    tag, wal_end, server_time_us, reply_requested = KEEPALIVE.unpack(buf)
    if tag != b"k":
        raise ReplicationProtocolError(f"Expected keepalive tag b'k', got {tag!r}")

    return wal_end, server_time_us, reply_requested


def build_standby_status(ack_lsn: int, *, reply_requested: int = 0) -> bytes:
    return STANDBY_STATUS.pack(
        b"r",
        ack_lsn,
        ack_lsn,
        ack_lsn,
        now_us(),
        1 if reply_requested else 0,
    )


def lsn_str_to_int(lsn: str) -> int:
    a, b = lsn.split("/")
    return (int(a, 16) << 32) | int(b, 16)


def lsn_int_to_str(lsn: int) -> str:
    return f"{(lsn >> 32):X}/{(lsn & 0xFFFFFFFF):X}"

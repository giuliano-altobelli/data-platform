from __future__ import annotations

import hashlib
import json
from typing import Any

from cdc_logical_replication.protocol import lsn_int_to_str

PARTITION_KEY_MAX_LEN = 256


def extract_partition_key(
    payload: bytes,
    *,
    lsn: int,
    mode: str,
    fallback: str,
    static_fallback_value: str | None,
) -> str:
    if mode == "primary_key":
        parsed_payload = _safe_parse(payload)
        primary = _primary_key_from_payload(parsed_payload)
        if primary:
            return _normalize_partition_key(primary)
        return _normalize_partition_key(
            _fallback_key(
                fallback=fallback,
                static_value=static_fallback_value,
                lsn=lsn,
                parsed_payload=parsed_payload,
            )
        )

    if mode == "fallback":
        return _normalize_partition_key(
            _fallback_key(
                fallback=fallback,
                static_value=static_fallback_value,
                lsn=lsn,
                parsed_payload=_safe_parse(payload),
            )
        )

    raise ValueError(f"Unsupported partition key mode: {mode}")


def _safe_parse(payload: bytes) -> dict[str, Any] | None:
    normalized = payload.rstrip(b"\n")
    if not normalized:
        return None

    try:
        decoded = json.loads(normalized)
    except json.JSONDecodeError:
        return None

    if not isinstance(decoded, dict):
        return None

    return decoded


def _primary_key_from_payload(parsed_payload: dict[str, Any] | None) -> str | None:
    if not parsed_payload:
        return None

    change = _first_change(parsed_payload)
    if not change:
        return None

    pk_entries = change.get("pk")
    if not isinstance(pk_entries, list) or not pk_entries:
        return None

    key_parts: list[str] = []
    for entry in pk_entries:
        if not isinstance(entry, dict):
            continue

        name = entry.get("name")
        if not isinstance(name, str):
            continue

        value = entry.get("value")
        key_parts.append(f"{name}={value}")

    if not key_parts:
        return None

    table_name = _table_name(change)
    return f"{table_name}:{'|'.join(key_parts)}"


def _fallback_key(
    *,
    fallback: str,
    static_value: str | None,
    lsn: int,
    parsed_payload: dict[str, Any] | None,
) -> str:
    if fallback == "lsn":
        return lsn_int_to_str(lsn)

    if fallback == "table":
        change = _first_change(parsed_payload)
        if change:
            table_name = _table_name(change)
            if table_name != "unknown.unknown":
                return table_name
        return lsn_int_to_str(lsn)

    if fallback == "static":
        if not static_value:
            raise ValueError("PARTITION_KEY_STATIC_VALUE must be set for static fallback mode")
        return static_value

    raise ValueError(f"Unsupported fallback partition key mode: {fallback}")


def _first_change(parsed_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not parsed_payload:
        return None

    change_records = parsed_payload.get("change")
    if not isinstance(change_records, list) or not change_records:
        return None

    first = change_records[0]
    if not isinstance(first, dict):
        return None

    return first


def _table_name(change: dict[str, Any]) -> str:
    schema = change.get("schema") if isinstance(change.get("schema"), str) else "unknown"
    table = change.get("table") if isinstance(change.get("table"), str) else "unknown"
    return f"{schema}.{table}"


def _normalize_partition_key(candidate: str) -> str:
    if not candidate:
        return "0"

    if len(candidate) <= PARTITION_KEY_MAX_LEN:
        return candidate

    return hashlib.sha256(candidate.encode("utf-8")).hexdigest()

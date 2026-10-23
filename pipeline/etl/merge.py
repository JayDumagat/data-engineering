"""Non-empty JSON merging and key-based deduplication for curated records."""

from __future__ import annotations

import json
from typing import Any

import polars as pl


def has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        return any(has_content(item) for item in value.values())
    if isinstance(value, list):
        return any(has_content(item) for item in value)
    return True


def merge_json(existing: Any, incoming: Any) -> Any:
    """Prefer newer values but keep older non-empty fields that the fetch omitted."""
    if isinstance(existing, dict) and isinstance(incoming, dict):
        merged = dict(existing)
        for key, value in incoming.items():
            merged[key] = merge_json(merged.get(key), value)
        return merged
    if isinstance(incoming, list) and not incoming:
        return existing if has_content(existing) else incoming
    if has_content(incoming):
        return incoming
    return existing if has_content(existing) else incoming


def deduplicate_frame(
    frame: pl.DataFrame,
    *,
    keys: tuple[str, ...] = ("dataset", "record_key"),
) -> pl.DataFrame:
    """Keep one current record per key, merging partial duplicate captures."""
    if frame.is_empty():
        return frame
    rows: dict[tuple[Any, ...], dict[str, Any]] = {}
    for incoming in frame.iter_rows(named=True):
        identity = tuple(incoming[column] for column in keys)
        current = rows.get(identity)
        if current is None:
            rows[identity] = incoming
            continue
        merged = {**current, **incoming}
        try:
            previous_payload = json.loads(current.get("payload_json") or "{}")
            incoming_payload = json.loads(incoming.get("payload_json") or "{}")
            merged["payload_json"] = json.dumps(
                merge_json(previous_payload, incoming_payload),
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            merged["payload_json"] = incoming.get("payload_json") or current.get("payload_json")
        if incoming.get("record_type") in {None, "", "unknown"}:
            merged["record_type"] = current.get("record_type") or "unknown"
        rows[identity] = merged
    return pl.DataFrame(list(rows.values()), schema=frame.schema, strict=False)

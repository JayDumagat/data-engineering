"""Validated Polars frame contract shared by data sinks."""

from __future__ import annotations

from typing import Any, Iterable, Mapping
import json
import logging

import pandera.polars as pa
import polars as pl


RECORD_FRAME_TYPES = {
    "record_id": pl.String,
    "dataset": pl.String,
    "record_type": pl.String,
    "record_key": pl.String,
    "source_url": pl.String,
    "raw_bucket": pl.String,
    "raw_key": pl.String,
    "payload_json": pl.String,
    "ingested_at": pl.Datetime("us", "UTC"),
}

RECORD_FRAME_SCHEMA = pa.DataFrameSchema(
    {
        "record_id": pa.Column(str, nullable=False),
        "dataset": pa.Column(str, nullable=False),
        "record_type": pa.Column(str, nullable=False),
        "record_key": pa.Column(str, nullable=False),
        "source_url": pa.Column(str, nullable=False),
        "raw_bucket": pa.Column(str, nullable=False),
        "raw_key": pa.Column(str, nullable=False),
        "payload_json": pa.Column(str, nullable=False),
        "ingested_at": pa.Column(pl.Datetime("us", "UTC"), nullable=False),
    },
    strict=True,
    name="normalized_records",
)

LOGGER = logging.getLogger(__name__)


def _has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping):
        return any(_has_content(item) for key, item in value.items() if key != "record_type")
    if isinstance(value, (list, tuple)):
        return any(_has_content(item) for item in value)
    return True


def records_to_frame(records: Iterable[Mapping[str, Any]]) -> pl.DataFrame:
    rows = []
    rejected = 0
    for record in records:
        payload = record.get("payload")
        required = (
            record.get("record_id"),
            record.get("dataset"),
            record.get("record_key"),
            record.get("source_url"),
            record.get("raw_bucket"),
            record.get("raw_key"),
            record.get("ingested_at"),
        )
        if not isinstance(payload, Mapping) or not _has_content(payload) or any(
            value is None or not str(value).strip() for value in required
        ):
            rejected += 1
            continue
        rows.append({
            "record_id": str(record["record_id"]),
            "dataset": str(record["dataset"]),
            "record_type": str(payload.get("record_type", "unknown")),
            "record_key": str(record["record_key"]),
            "source_url": str(record["source_url"]),
            "raw_bucket": str(record["raw_bucket"]),
            "raw_key": str(record["raw_key"]),
            "payload_json": json.dumps(payload, ensure_ascii=False, default=str),
            "ingested_at": record["ingested_at"],
        })
    if rejected:
        LOGGER.warning("Rejected %d empty or invalid normalized source records", rejected)
    frame = pl.DataFrame(rows, schema=RECORD_FRAME_TYPES, strict=False)
    return RECORD_FRAME_SCHEMA.validate(frame, lazy=True)

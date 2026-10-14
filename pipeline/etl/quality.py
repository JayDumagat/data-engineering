"""Validated Polars frame contract shared by data sinks."""

from __future__ import annotations

from typing import Any, Iterable, Mapping
import json

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


def records_to_frame(records: Iterable[Mapping[str, Any]]) -> pl.DataFrame:
    rows = [
        {
            "record_id": str(record["record_id"]),
            "dataset": str(record["dataset"]),
            "record_type": str(record.get("payload", {}).get("record_type", "unknown")),
            "record_key": str(record["record_key"]),
            "source_url": str(record["source_url"]),
            "raw_bucket": str(record["raw_bucket"]),
            "raw_key": str(record["raw_key"]),
            "payload_json": json.dumps(record["payload"], ensure_ascii=False, default=str),
            "ingested_at": record["ingested_at"],
        }
        for record in records
    ]
    frame = pl.DataFrame(rows, schema=RECORD_FRAME_TYPES, strict=False)
    return RECORD_FRAME_SCHEMA.validate(frame, lazy=True)

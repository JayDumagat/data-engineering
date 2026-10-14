"""Polars Parquet sink for raw-stage and curated object-store layers."""

from __future__ import annotations

from datetime import timezone
from io import BytesIO
from typing import Any
import re

import polars as pl

from pipeline.core.contracts import ObjectStore, RunContext


def _safe_segment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value).strip("_") or "unknown"


class ParquetLayerSink:
    def __init__(self, *, name: str, bucket: str, layer: str, object_store: ObjectStore) -> None:
        self.name = name
        self.bucket = bucket
        self.layer = layer
        self.object_store = object_store

    def write(self, records: pl.DataFrame, context: RunContext) -> dict[str, Any]:
        objects = []
        for dataset_frame in records.partition_by("dataset", maintain_order=True):
            dataset = str(dataset_frame.get_column("dataset").item(0))
            ingest_date = context.started_at.astimezone(timezone.utc).date().isoformat()
            key = (
                f"{_safe_segment(context.source)}/{self.layer}/dataset={_safe_segment(dataset)}/"
                f"ingest_date={ingest_date}/run_id={_safe_segment(context.run_id)}/part-00000.parquet"
            )
            output = BytesIO()
            dataset_frame.write_parquet(output, compression="zstd")
            self.object_store.write(
                self.bucket,
                key,
                output.getvalue(),
                content_type="application/vnd.apache.parquet",
            )
            objects.append(
                {
                    "dataset": dataset,
                    "bucket": self.bucket,
                    "key": key,
                    "records": dataset_frame.height,
                }
            )
        return {"records_written": records.height, "objects": objects}


def read_parquet_objects(objects: list[dict[str, Any]], object_store: ObjectStore) -> pl.DataFrame:
    frames = [
        pl.read_parquet(BytesIO(object_store.read(item["bucket"], item["key"])))
        for item in objects
    ]
    return pl.concat(frames, how="vertical") if frames else pl.DataFrame()

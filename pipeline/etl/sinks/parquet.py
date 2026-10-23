"""Polars Parquet sink for raw-stage and curated object-store layers."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import timezone
from functools import lru_cache
from io import BytesIO
from typing import Any
import re
import logging

import polars as pl
from sqlalchemy import text

from pipeline.core.contracts import ObjectStore, RunContext
from pipeline.etl.merge import deduplicate_frame
from pipeline.storage.warehouse import get_warehouse_engine


LOGGER = logging.getLogger(__name__)


@contextmanager
def _snapshot_lock(source: str, dataset: str):
    """Serialize read-merge-write updates to one shared curated snapshot."""
    with _snapshot_lock_engine().begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:source), hashtext(:dataset))"),
            {"source": source, "dataset": dataset},
        )
        yield


@lru_cache(maxsize=1)
def _snapshot_lock_engine():
    return get_warehouse_engine()


def _safe_segment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value).strip("_") or "unknown"


def _missing_object_error(exc: Exception) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", "")) if isinstance(response, dict) else ""
    return code in {"NoSuchKey", "NoSuchBucket", "404", "NotFound"}


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
            base_key = f"{_safe_segment(context.source)}/{self.layer}/dataset={_safe_segment(dataset)}"
            delta_key = (
                f"{base_key}/ingest_date={ingest_date}/run_id={_safe_segment(context.run_id)}/"
                f"part-00000.parquet"
            )
            if self.layer == "curated":
                with _snapshot_lock(context.source, dataset):
                    current_key = (
                        f"{_safe_segment(context.source)}/curated/current/"
                        f"dataset={_safe_segment(dataset)}/current.parquet"
                    )
                    dataset_frame = deduplicate_frame(dataset_frame)
                    current_frame = dataset_frame.head(0)
                    try:
                        current_frame = pl.read_parquet(
                            BytesIO(self.object_store.read(self.bucket, current_key))
                        )
                    except Exception as exc:
                        if not _missing_object_error(exc):
                            raise
                        list_objects = getattr(self.object_store, "list_objects", None)
                        if callable(list_objects):
                            legacy_prefix = f"{base_key}/"
                            legacy_keys = [
                                key
                                for key in list_objects(self.bucket, legacy_prefix)
                                if key.endswith(".parquet") and key != current_key
                            ]
                            legacy_frames = [
                                pl.read_parquet(BytesIO(self.object_store.read(self.bucket, key)))
                                for key in legacy_keys
                            ]
                            if legacy_frames:
                                current_frame = deduplicate_frame(
                                    pl.concat([*legacy_frames, dataset_frame], how="vertical_relaxed")
                                )
                                LOGGER.info(
                                    "Bootstrapped curated snapshot from legacy run files: source=%s dataset=%s objects=%d records=%d",
                                    context.source,
                                    dataset,
                                    len(legacy_frames),
                                    current_frame.height,
                                )
                            else:
                                current_frame = deduplicate_frame(dataset_frame)
                        else:
                            current_frame = deduplicate_frame(dataset_frame)
                    else:
                        current_frame = deduplicate_frame(
                            pl.concat([current_frame, dataset_frame], how="vertical_relaxed")
                        )
                    self._write_frame(current_key, current_frame)
                    objects.append(
                        {
                            "dataset": dataset,
                            "bucket": self.bucket,
                            "key": current_key,
                            "records": current_frame.height,
                            "new_records": dataset_frame.height,
                            "role": "snapshot",
                        }
                    )
            else:
                self._write_frame(delta_key, dataset_frame)
                objects.append(
                    {
                        "dataset": dataset,
                        "bucket": self.bucket,
                        "key": delta_key,
                        "records": dataset_frame.height,
                        "role": "run",
                    }
                )
        return {"records_written": records.height, "objects": objects}

    def _write_frame(self, key: str, frame: pl.DataFrame) -> None:
        output = BytesIO()
        frame.write_parquet(output, compression="zstd")
        self.object_store.write(
            self.bucket,
            key,
            output.getvalue(),
            content_type="application/vnd.apache.parquet",
        )


def read_parquet_objects(objects: list[dict[str, Any]], object_store: ObjectStore) -> pl.DataFrame:
    frames = [
        pl.read_parquet(BytesIO(object_store.read(item["bucket"], item["key"])))
        for item in objects
    ]
    return pl.concat(frames, how="vertical") if frames else pl.DataFrame()

"""Bounded, ordered transformation of raw source objects."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator, Mapping
import logging

from pipeline.core.concurrency import bounded_map
from pipeline.core.contracts import RawObjectReader, Transformer
from pipeline.etl.extract import read_source_object


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransformOutcome:
    records: list[dict[str, Any]]
    failure: dict[str, Any] | None = None


def iter_transformed_objects(
    descriptors: list[Mapping[str, Any]],
    *,
    reader: RawObjectReader,
    transformer: Transformer,
    ingested_at: datetime,
    workers: int,
) -> Iterator[TransformOutcome]:
    """Read and transform objects on a bounded pool, preserving source order."""

    def process(descriptor: Mapping[str, Any]) -> TransformOutcome:
        try:
            body = read_source_object(descriptor, reader)
            records = transformer.transform(descriptor, body, ingested_at)
            return TransformOutcome(records=records)
        except Exception as exc:
            LOGGER.exception("Could not transform raw object %s", descriptor.get("raw_key"))
            return TransformOutcome(
                records=[],
                failure={
                    "dataset": descriptor.get("dataset"),
                    "url": descriptor.get("url"),
                    "raw_bucket": descriptor.get("raw_bucket"),
                    "raw_key": descriptor.get("raw_key"),
                    "error": str(exc),
                    "required": False,
                },
            )

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="etl-transform") as executor:
        yield from bounded_map(
            executor,
            process,
            descriptors,
            max_pending=workers * 2,
        )

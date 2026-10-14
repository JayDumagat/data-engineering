"""Route a validated record frame to independent sink adapters."""

from __future__ import annotations

from typing import Any
import logging

import polars as pl

from pipeline.core.contracts import RunContext
from pipeline.etl.routing import DatasetRouter
from pipeline.etl.sinks.registry import SinkRegistry


LOGGER = logging.getLogger(__name__)


def load_to_sinks(
    frame: pl.DataFrame,
    *,
    context: RunContext,
    sinks: SinkRegistry,
    router: DatasetRouter,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for sink in sinks:
        routed = router.for_sink(frame, sink.name)
        result = sink.write(routed, context)
        results[sink.name] = result
        LOGGER.info(
            "ETL sink completed: source=%s run_id=%s sink=%s records=%s",
            context.source,
            context.run_id,
            sink.name,
            result.get("records_written", 0),
        )
    return results

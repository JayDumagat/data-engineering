"""PSE EDGE parser adapter for the shared ETL transformer contract."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from pipeline.etl.pse_edge.normalize import normalize_object, normalize_rss_items


class PSEEdgeTransformer:
    def transform(
        self,
        descriptor: Mapping[str, Any],
        body: bytes | None,
        ingested_at: datetime,
    ) -> list[dict[str, Any]]:
        if descriptor.get("dataset") == "rss" and descriptor.get("rss_items") is not None:
            return normalize_rss_items(dict(descriptor), descriptor["rss_items"], ingested_at)
        if body is None:
            raise ValueError("Raw body is required when source items are not stored inline")
        return normalize_object(dict(descriptor), body, ingested_at)

"""Convert PSE website captures into shared normalized records."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from pipeline.etl.pse_edge.normalize import normalize_object, normalize_payload


class PSEWebsiteTransformer:
    def transform(
        self,
        descriptor: Mapping[str, Any],
        body: bytes | None,
        ingested_at: datetime,
    ) -> list[dict[str, Any]]:
        normalized = dict(descriptor)
        if descriptor.get("inline_records") is not None:
            payloads = list(descriptor["inline_records"])
        else:
            if body is None:
                raise ValueError("Raw body is required for this PSE website object")
            normalized_records = normalize_object(normalized, body, ingested_at)
            normalized_records.extend(
                normalize_payload(
                    normalized,
                    {
                        "record_type": "outbound_link",
                        "id": f"{descriptor['url']}#{link}",
                        "url": link,
                    },
                    ingested_at,
                )
                for link in descriptor.get("outlinks", [])
            )
            return normalized_records
        return [normalize_payload(normalized, payload, ingested_at) for payload in payloads]

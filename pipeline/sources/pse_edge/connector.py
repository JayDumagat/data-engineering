"""PSE EDGE implementation of the shared source connector contract."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import re

from pipeline.core.contracts import IngestionRequest, ManifestRef
from pipeline.etl.extract import read_manifest
from pipeline.sources.pse_edge.scraper import scrape
from pipeline.storage.object_storage import S3ObjectStore, upload_bytes


class PSEEdgeConnector:
    name = "pse_edge"

    def extract(self, request: IngestionRequest) -> ManifestRef:
        result = scrape(
            scope=request.scope,
            run_id=request.run_id,
            feed_urls=request.options.get("feed_urls"),
            max_concurrency=request.options.get("max_concurrency"),
        )
        return ManifestRef(
            source=self.name,
            scope=request.scope,
            run_id=result["run_id"],
            bucket=result["bucket"],
            key=result["manifest_key"],
        )


def merge_rss_manifests(
    references: list[dict[str, str]],
    *,
    run_id: str,
) -> dict[str, str]:
    """Combine mapped per-feed captures into one ETL input manifest."""
    if not references:
        raise ValueError("RSS ingestion produced no feed manifests")

    object_store = S3ObjectStore()
    manifests = [
        read_manifest(ManifestRef.from_mapping(reference), object_store)
        for reference in references
    ]
    started_at = min(datetime.fromisoformat(item["started_at"]) for item in manifests)
    completed_at = datetime.now(timezone.utc)
    objects = [obj for manifest in manifests for obj in manifest.get("objects", [])]
    failures = [failure for manifest in manifests for failure in manifest.get("failures", [])]
    merged = {
        "schema_version": 1,
        "source": "pse_edge",
        "scope": "rss",
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "base_url": os.getenv("PSE_EDGE_BASE_URL", "https://edge.pse.com.ph").rstrip("/"),
        "objects": objects,
        "failures": failures,
        "records_hint": sum(int(manifest.get("records_hint", 0)) for manifest in manifests),
        "feed_manifests": [
            {"bucket": reference["bucket"], "key": reference["key"]}
            for reference in references
        ],
    }
    raw_bucket = os.getenv("S3_BUCKET_RAW", "raw")
    run_segment = re.sub(r"[^A-Za-z0-9_.=-]+", "_", run_id).strip("_")
    manifest_key = (
        f"pse_edge/manifests/ingest_date={started_at:%Y-%m-%d}/"
        f"run_id={run_segment or 'rss'}.json"
    )
    upload_bytes(
        bucket=raw_bucket,
        key=manifest_key,
        data=json.dumps(merged, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        content_type="application/json",
    )
    return ManifestRef(
        source="pse_edge",
        scope="rss",
        run_id=run_id,
        bucket=raw_bucket,
        key=manifest_key,
    ).to_dict()

"""Object-store sink for recoverable transformation and validation failures."""

from __future__ import annotations

from datetime import timezone
from typing import Any
import json
import re

from pipeline.core.contracts import ObjectStore, RunContext


class QuarantineSink:
    name = "quarantine"

    def __init__(self, *, bucket: str, object_store: ObjectStore) -> None:
        self.bucket = bucket
        self.object_store = object_store

    def write(self, failures: list[dict[str, Any]], context: RunContext) -> dict[str, Any]:
        ingest_date = context.started_at.astimezone(timezone.utc).date().isoformat()
        safe_run_id = re.sub(r"[^A-Za-z0-9_.=-]+", "_", context.run_id)
        key = f"{context.source}/ingest_date={ingest_date}/run_id={safe_run_id}/transform_errors.json"
        body = json.dumps(failures, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
        self.object_store.write(self.bucket, key, body, content_type="application/json")
        return {"records_written": len(failures), "bucket": self.bucket, "key": key}

"""PSE public website implementation of the shared source connector contract."""

from __future__ import annotations

from pipeline.core.contracts import IngestionRequest, ManifestRef, ObjectStore
from pipeline.sources.pse_website.crawler import crawl_public_site
from pipeline.storage.object_storage import get_object_store


class PSEWebsiteConnector:
    name = "pse_website"

    def __init__(self, object_store: ObjectStore | None = None) -> None:
        self.object_store = object_store if object_store is not None else get_object_store()

    def extract(self, request: IngestionRequest) -> ManifestRef:
        if request.source != self.name:
            raise ValueError(f"Expected source {self.name!r}; received {request.source!r}")
        result = crawl_public_site(
            run_id=request.run_id,
            scope=request.scope,
            options=dict(request.options),
            object_store=self.object_store,
        )
        return ManifestRef(
            source=self.name,
            scope=result["scope"],
            run_id=result["run_id"],
            bucket=result["bucket"],
            key=result["manifest_key"],
        )

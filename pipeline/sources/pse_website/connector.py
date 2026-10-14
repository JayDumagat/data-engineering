"""PSE public website implementation of the shared source connector contract."""

from __future__ import annotations

from pipeline.core.contracts import IngestionRequest, ManifestRef
from pipeline.sources.pse_website.crawler import crawl_public_site


class PSEWebsiteConnector:
    name = "pse_website"

    def extract(self, request: IngestionRequest) -> ManifestRef:
        if request.source != self.name:
            raise ValueError(f"Expected source {self.name!r}; received {request.source!r}")
        result = crawl_public_site(run_id=request.run_id, options=dict(request.options))
        return ManifestRef(
            source=self.name,
            scope=request.scope,
            run_id=result["run_id"],
            bucket=result["bucket"],
            key=result["manifest_key"],
        )

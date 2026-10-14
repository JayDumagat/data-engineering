"""Fetch one PSE EDGE announcements page into the raw object-store bucket."""

from datetime import datetime, timezone
import json
import os
import uuid

from pipeline.sources.pse_edge.client import PSEEdgeClient
from pipeline.storage.object_storage import upload_bytes


def fetch_announcement_page() -> dict[str, object]:
    base_url = os.getenv("PSE_EDGE_BASE_URL", "https://edge.pse.com.ph").rstrip("/")
    url = f"{base_url}/announcements/form.do"
    with PSEEdgeClient() as client:
        response = client.get(url)

    now = datetime.now(timezone.utc)
    key = (
        "pse_edge/announcements/"
        f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
        f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}.html"
    )
    bucket = os.getenv("S3_BUCKET_RAW", "raw")
    upload_bytes(
        bucket=bucket,
        key=key,
        data=response.body,
        content_type="text/html",
    )
    return {"bucket": bucket, "key": key, "url": url, "status": response.status}


if __name__ == "__main__":
    print(json.dumps(fetch_announcement_page(), indent=2))

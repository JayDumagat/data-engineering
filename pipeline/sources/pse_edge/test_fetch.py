from datetime import datetime, timezone
import os

from scrapling.fetchers import Fetcher

from pipeline.storage.object_storage import upload_bytes


def scrape():
    url = "https://edge.pse.com.ph/announcements/form.do"

    response = Fetcher.get(
        url,
        impersonate="chrome",
        timeout=30,
    )

    now = datetime.now(timezone.utc)

    key = (
        "pse-edge/"
        "announcements/"
        f"year={now:%Y}/"
        f"month={now:%m}/"
        f"day={now:%d}/"
        f"hour={now:%H}/"
        f"{now:%Y%m%dT%H%M%SZ}.html"
    )

    upload_bytes(
        bucket=os.getenv("S3_BUCKET_RAW", "raw"),
        key=key,
        data=response.body,
        content_type="text/html",
    )

    return {
        "bucket": os.getenv("S3_BUCKET_RAW", "raw"),
        "key": key,
        "url": url,
        "status": response.status,
    }
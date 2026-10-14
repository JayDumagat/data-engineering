"""Run one bounded capture of the public PSE website into raw object storage."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json

from pipeline.core.contracts import IngestionRequest
from pipeline.sources.pse_website.connector import PSEWebsiteConnector


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-pages", type=int, default=3000)
    parser.add_argument("--max-assets", type=int, default=3000)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    run_id = f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    reference = PSEWebsiteConnector().extract(
        IngestionRequest(
            source="pse_website",
            scope="public_site",
            run_id=run_id,
            options={
                "max_pages": args.max_pages,
                "max_assets": args.max_assets,
                "max_concurrency": args.workers,
            },
        )
    )
    print(json.dumps(reference.to_dict(), indent=2))


if __name__ == "__main__":
    main()

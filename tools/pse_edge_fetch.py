"""Capture one PSE EDGE data job into raw object storage."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json

from pipeline.core.contracts import IngestionRequest
from pipeline.sources.pse_edge.catalog import PSE_EDGE_JOBS
from pipeline.sources.pse_edge.connector import PSEEdgeConnector


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope",
        choices=("configured_sources", "all", "rss", *PSE_EDGE_JOBS),
        default="configured_sources",
        help="Capture all catalog sources by default; 'all' is a legacy alias for configured_sources.",
    )
    parser.add_argument("--workers", type=int, default=None, help="Optional request-worker override")
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Leave date filters open when running a named job",
    )
    args = parser.parse_args()

    run_id = f"manual__{args.scope}__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    reference = PSEEdgeConnector().extract(
        IngestionRequest(
            source="pse_edge",
            scope=args.scope,
            run_id=run_id,
            options={"max_concurrency": args.workers, "full_history": args.full_history},
        )
    )
    print(json.dumps(reference.to_dict(), indent=2))


if __name__ == "__main__":
    main()

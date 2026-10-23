"""Dataset and record-type routing to independent storage destinations."""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl


@dataclass(frozen=True)
class DatasetRouter:
    """Resolve a data product's sinks without coupling it to any sink type."""

    default_sinks: frozenset[str] = frozenset(
        {"stage_parquet", "curated_parquet", "postgres"}
    )
    dataset_sinks: dict[str, frozenset[str]] = field(
        default_factory=lambda: {
            # The source bytes are already stored as objects; keep only their
            # searchable metadata in the lake instead of copying it into SQL.
            "disclosure_documents": frozenset({"stage_parquet", "curated_parquet"}),
            "disclosure_assets": frozenset({"stage_parquet", "curated_parquet"}),
            "website_pages": frozenset({"stage_parquet", "curated_parquet"}),
            "website_sitemaps": frozenset({"stage_parquet", "curated_parquet"}),
            "website_assets": frozenset({"stage_parquet", "curated_parquet"}),
            "website_crawl_controls": frozenset({"stage_parquet", "curated_parquet"}),
        }
    )
    record_type_sinks: dict[str, frozenset[str]] = field(
        default_factory=lambda: {
            "binary_asset": frozenset({"stage_parquet", "curated_parquet"}),
            "image_asset": frozenset({"stage_parquet", "curated_parquet"}),
            "video_asset": frozenset({"stage_parquet", "curated_parquet"}),
            "page_snapshot": frozenset({"stage_parquet", "curated_parquet"}),
            "outbound_link": frozenset({"stage_parquet", "curated_parquet"}),
            "sitemap_url": frozenset({"stage_parquet", "curated_parquet"}),
            "robots_policy": frozenset({"stage_parquet", "curated_parquet"}),
            "crawl_summary": frozenset({"stage_parquet", "curated_parquet"}),
            "source_table_row": frozenset({"stage_parquet", "curated_parquet"}),
        }
    )

    def for_sink(self, frame: pl.DataFrame, sink_name: str) -> pl.DataFrame:
        if frame.is_empty():
            return frame
        selected = []
        for dataset in frame.get_column("dataset").unique(maintain_order=True).to_list():
            dataset_frame = frame.filter(pl.col("dataset") == dataset)
            dataset_route = self.dataset_sinks.get(dataset, self.default_sinks)
            for record_type in dataset_frame.get_column("record_type").unique(maintain_order=True).to_list():
                destinations = dataset_route & self.record_type_sinks.get(record_type, dataset_route)
                if sink_name in destinations:
                    selected.append(
                        dataset_frame.filter(pl.col("record_type") == record_type)
                    )
        if not selected:
            return frame.head(0)
        return pl.concat(selected, how="vertical")

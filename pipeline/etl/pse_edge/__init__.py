"""PSE EDGE transformation and warehouse loading API."""

from typing import Any


def transform_and_load(manifest_bucket: str, manifest_key: str) -> dict[str, Any]:
    """Load a source manifest through the PSE EDGE ETL pipeline."""
    from pipeline.etl.pipeline import transform_and_load as run_pipeline

    return run_pipeline(manifest_bucket, manifest_key)


def transform_to_lake(manifest_bucket: str, manifest_key: str) -> dict[str, Any]:
    """Run extraction, transformation, validation, and lake writes."""
    from pipeline.etl.pipeline import transform_to_lake as run_transform

    return run_transform(manifest_bucket, manifest_key)


def load_curated(manifest_bucket: str, manifest_key: str) -> dict[str, Any]:
    """Load routed curated objects to configured relational sinks."""
    from pipeline.etl.pipeline import load_curated as run_load

    return run_load(manifest_bucket, manifest_key)

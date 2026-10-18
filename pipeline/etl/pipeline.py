"""Shared ETL runner for registered sources and sink adapters."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import json
import logging
import os
import re

from pipeline.core.contracts import ManifestRef, ObjectStore, RunContext
from pipeline.etl.extract import read_manifest
from pipeline.etl.load import load_to_sinks
from pipeline.etl.pse_edge.transformer import PSEEdgeTransformer
from pipeline.etl.quality import records_to_frame
from pipeline.etl.routing import DatasetRouter
from pipeline.etl.sinks.parquet import ParquetLayerSink, read_parquet_objects
from pipeline.etl.sinks.postgres import PostgresRecordSink
from pipeline.etl.sinks.quarantine import QuarantineSink
from pipeline.etl.sinks.registry import SinkRegistry
from pipeline.etl.transform import iter_transformed_objects
from pipeline.storage.object_storage import get_object_store
from pipeline.storage.warehouse import (
    ensure_source_schema,
    get_warehouse_engine,
    record_ingestion_run,
)


LOGGER = logging.getLogger(__name__)


def _transformer_for_source(source: str):
    if source == "pse_edge":
        return PSEEdgeTransformer()
    if source == "pse_website":
        from pipeline.etl.pse_website.transformer import PSEWebsiteTransformer

        return PSEWebsiteTransformer()
    raise ValueError(f"No transformer is registered for source {source!r}")


def _configured_workers() -> int:
    try:
        return min(16, max(1, int(os.getenv("PSE_ETL_WORKERS", "4"))))
    except ValueError:
        LOGGER.warning("Invalid PSE_ETL_WORKERS; using 4 workers")
        return 4


def _run_context(manifest: dict[str, Any], reference: ManifestRef) -> RunContext:
    return RunContext(
        source=manifest.get("source", reference.source),
        scope=manifest.get("scope", reference.scope),
        run_id=manifest["run_id"],
        started_at=datetime.fromisoformat(manifest["started_at"]).astimezone(timezone.utc),
        completed_at=datetime.fromisoformat(manifest["completed_at"]).astimezone(timezone.utc),
        manifest=reference,
    )


def transform_to_lake(
    manifest_bucket: str,
    manifest_key: str,
    *,
    object_store: ObjectStore | None = None,
) -> dict[str, Any]:
    """Read raw objects, normalize/validate them, and write stage/curated Parquet."""
    object_store = object_store if object_store is not None else get_object_store()
    reference = ManifestRef("pse_edge", "all", "unknown", manifest_bucket, manifest_key)
    manifest = read_manifest(reference, object_store)
    reference = ManifestRef(
        manifest.get("source", "pse_edge"),
        manifest.get("scope", "all"),
        manifest["run_id"],
        manifest_bucket,
        manifest_key,
    )
    context = _run_context(manifest, reference)
    worker_count = _configured_workers()
    LOGGER.info(
        "ETL transform started: source=%s run_id=%s raw_objects=%d workers=%d",
        context.source,
        context.run_id,
        len(manifest.get("objects", [])),
        worker_count,
    )

    records: list[dict[str, Any]] = []
    transform_failures: list[dict[str, Any]] = []
    for outcome in iter_transformed_objects(
        manifest.get("objects", []),
        reader=object_store,
        transformer=_transformer_for_source(context.source),
        ingested_at=context.started_at,
        workers=worker_count,
    ):
        records.extend(outcome.records)
        if outcome.failure:
            transform_failures.append(outcome.failure)

    stage_frame = records_to_frame(records)
    curated_frame = stage_frame.unique(
        subset=["dataset", "record_key"],
        keep="last",
        maintain_order=True,
    )
    router = DatasetRouter()
    stage_bucket = os.getenv("S3_BUCKET_STAGE", "stage")
    curated_bucket = os.getenv("S3_BUCKET_CURATED", "curated")
    stage_sink = ParquetLayerSink(
        name="stage_parquet",
        bucket=stage_bucket,
        layer="stage",
        object_store=object_store,
    )
    curated_sink = ParquetLayerSink(
        name="curated_parquet",
        bucket=curated_bucket,
        layer="curated",
        object_store=object_store,
    )
    stage_result = load_to_sinks(
        stage_frame,
        context=context,
        sinks=SinkRegistry([stage_sink]),
        router=router,
    )[stage_sink.name]
    curated_result = load_to_sinks(
        curated_frame,
        context=context,
        sinks=SinkRegistry([curated_sink]),
        router=router,
    )[curated_sink.name]

    failures = list(manifest.get("failures", [])) + transform_failures
    if transform_failures:
        quarantine = QuarantineSink(
            bucket=os.getenv("S3_BUCKET_QUARANTINE", "quarantine"),
            object_store=object_store,
        )
        quarantine.write(transform_failures, context)

    run_id_key = re.sub(r"[^A-Za-z0-9_.=-]+", "_", context.run_id)
    ingest_date = context.started_at.date().isoformat()
    etl_manifest = {
        "schema_version": 1,
        "source": context.source,
        "scope": context.scope,
        "run_id": context.run_id,
        "started_at": context.started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "source_manifest": {"bucket": manifest_bucket, "key": manifest_key},
        "raw_object_count": len(manifest.get("objects", [])),
        "stage_record_count": stage_frame.height,
        "curated_record_count": curated_frame.height,
        "failures": failures,
        "outputs": {
            "stage_parquet": stage_result.get("objects", []),
            "curated_parquet": curated_result.get("objects", []),
        },
    }
    etl_manifest_key = (
        f"{context.source}/etl_manifests/ingest_date={ingest_date}/"
        f"run_id={run_id_key}.json"
    )
    object_store.write(
        stage_bucket,
        etl_manifest_key,
        json.dumps(etl_manifest, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8"),
        content_type="application/json",
    )
    result = {
        "source": context.source,
        "scope": context.scope,
        "run_id": context.run_id,
        "bucket": stage_bucket,
        "key": etl_manifest_key,
        "raw_objects": len(manifest.get("objects", [])),
        "stage_records": stage_frame.height,
        "curated_records": curated_frame.height,
        "failures": len(failures),
    }
    LOGGER.info("ETL transform complete: %s", result)
    return result


def load_curated(
    etl_manifest_bucket: str,
    etl_manifest_key: str,
    *,
    warehouse_engine: Any | None = None,
    object_store: ObjectStore | None = None,
) -> dict[str, Any]:
    """Load routed curated rows to SQL and persist the ingestion audit row."""
    object_store = object_store if object_store is not None else get_object_store()
    reference = ManifestRef("pse_edge", "all", "unknown", etl_manifest_bucket, etl_manifest_key)
    manifest = read_manifest(reference, object_store)
    source_reference = ManifestRef(
        manifest["source"],
        manifest["scope"],
        manifest["run_id"],
        manifest["source_manifest"]["bucket"],
        manifest["source_manifest"]["key"],
    )
    context = RunContext(
        source=manifest["source"],
        scope=manifest["scope"],
        run_id=manifest["run_id"],
        started_at=datetime.fromisoformat(manifest["started_at"]).astimezone(timezone.utc),
        completed_at=datetime.fromisoformat(manifest["completed_at"]).astimezone(timezone.utc),
        manifest=source_reference,
    )
    failures = manifest.get("failures", [])
    required_failures = [failure for failure in failures if failure.get("required")]
    status = "failed" if required_failures else ("partial" if failures else "success")

    owns_engine = warehouse_engine is None
    engine = warehouse_engine if warehouse_engine is not None else get_warehouse_engine()
    try:
        ensure_source_schema(engine, context.source)
        postgres_sink = PostgresRecordSink(engine=engine, ensure_schema=False)
        sinks = SinkRegistry([postgres_sink])
        warehouse_records = 0
        curated_objects = manifest.get("outputs", {}).get("curated_parquet", [])
        for curated_object in curated_objects:
            curated_frame = read_parquet_objects([curated_object], object_store)
            load_results = load_to_sinks(
                curated_frame,
                context=context,
                sinks=sinks,
                router=DatasetRouter(),
            )
            warehouse_records += load_results["postgres"].get("records_written", 0)
        record_ingestion_run(
            engine,
            {
                "run_id": context.run_id,
                "scope": context.scope,
                "status": status,
                "started_at": context.started_at,
                "completed_at": context.completed_at,
                "manifest_bucket": source_reference.bucket,
                "manifest_key": source_reference.key,
                "raw_object_count": manifest.get("raw_object_count", 0),
                "stage_record_count": manifest.get("stage_record_count", 0),
                "curated_record_count": manifest.get("curated_record_count", 0),
                "failures": failures,
            },
            source=context.source,
        )
    finally:
        if owns_engine:
            engine.dispose()

    result = {
        "source": context.source,
        "scope": context.scope,
        "run_id": context.run_id,
        "status": status,
        "raw_objects": manifest.get("raw_object_count", 0),
        "stage_records": manifest.get("stage_record_count", 0),
        "curated_records": manifest.get("curated_record_count", 0),
        "warehouse_records": warehouse_records,
        "failures": len(failures),
        "manifest": f"s3://{etl_manifest_bucket}/{etl_manifest_key}",
    }
    LOGGER.info("ETL load complete: %s", result)
    if required_failures:
        raise RuntimeError(
            f"{context.source} ingestion run {context.run_id} had {len(required_failures)} "
            "required source failures; partial data and run diagnostics are stored"
        )
    return result


def transform_and_load(
    manifest_bucket: str,
    manifest_key: str,
    *,
    object_store: ObjectStore | None = None,
) -> dict[str, Any]:
    """Backward-compatible synchronous entry point for non-Airflow callers."""
    lake_store = object_store if object_store is not None else get_object_store()
    transformed = transform_to_lake(
        manifest_bucket,
        manifest_key,
        object_store=lake_store,
    )
    loaded = load_curated(
        transformed["bucket"],
        transformed["key"],
        object_store=lake_store,
    )
    return {**transformed, **loaded}

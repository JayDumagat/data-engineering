"""Airflow orchestration for PSE EDGE capture and warehouse loading."""

from datetime import timedelta
import logging
import math
import os

import pendulum
from airflow.sdk import dag, get_current_context, task, task_group

from pse_edge_assets import PSE_EDGE_WAREHOUSE


MANILA_START = pendulum.datetime(2026, 1, 1, tz="Asia/Manila")
LOGGER = logging.getLogger(__name__)


def _source_pool_slots() -> int:
    try:
        slots = int(os.getenv("PSE_SOURCE_POOL_SLOTS", "6"))
        if slots < 1:
            raise ValueError("slot count must be positive")
        return slots
    except ValueError:
        LOGGER.warning("Invalid PSE_SOURCE_POOL_SLOTS; using 6 slots")
        return 6


SOURCE_POOL_SLOTS = _source_pool_slots()


def _source_job_slots() -> int:
    default_slots = min(2, SOURCE_POOL_SLOTS)
    try:
        slots = int(os.getenv("PSE_EDGE_JOB_SLOTS") or default_slots)
        if slots < 1:
            raise ValueError("slot count must be positive")
        return min(slots, SOURCE_POOL_SLOTS)
    except ValueError:
        LOGGER.warning("Invalid PSE_EDGE_JOB_SLOTS; using %d slots", default_slots)
        return default_slots


SOURCE_JOB_SLOTS = _source_job_slots()


def _source_job_request_delay() -> float:
    try:
        base_delay = float(os.getenv("PSE_REQUEST_DELAY_SECONDS", "0.5"))
        if not math.isfinite(base_delay) or base_delay < 0:
            raise ValueError("request delay must be finite and non-negative")
    except ValueError:
        LOGGER.warning("Invalid PSE_REQUEST_DELAY_SECONDS; using 0.5 seconds")
        base_delay = 0.5
    parallel_jobs = max(1, SOURCE_POOL_SLOTS // SOURCE_JOB_SLOTS)
    return base_delay * parallel_jobs


SOURCE_JOB_REQUEST_DELAY = _source_job_request_delay()


def _capture(
    scope: str,
    *,
    run_id: str | None = None,
    options: dict[str, object] | None = None,
) -> dict[str, str]:
    from pipeline.core.contracts import IngestionRequest
    from pipeline.sources.pse_edge.connector import PSEEdgeConnector
    from pipeline.storage.object_storage import get_object_store

    ti = get_current_context()["ti"]
    pipeline_run_id = run_id or f"{ti.dag_id}__{scope}__{ti.run_id}__try_{ti.try_number}"
    LOGGER.info(
        "Source extract started: dag_id=%s task_id=%s run_id=%s pipeline_run_id=%s "
        "try=%s source=pse_edge scope=%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        pipeline_run_id,
        ti.try_number,
        scope,
    )
    try:
        object_store = get_object_store()
        reference = PSEEdgeConnector(object_store=object_store).extract(
            IngestionRequest(
                source="pse_edge",
                scope=scope,
                run_id=pipeline_run_id,
                options=options or {},
            )
        )
        if (options or {}).get("fail_on_required_failures"):
            from pipeline.etl.extract import read_manifest

            source_manifest = read_manifest(reference, object_store)
            required_failures = [
                failure for failure in source_manifest.get("failures", [])
                if failure.get("required")
            ]
            if required_failures:
                raise RuntimeError(
                    f"Capture recorded {len(required_failures)} required source failures; "
                    f"diagnostics=s3://{reference.bucket}/{reference.key}"
                )
    except Exception:
        LOGGER.exception(
            "Source extract failed: dag_id=%s task_id=%s run_id=%s source=pse_edge scope=%s",
            ti.dag_id,
            ti.task_id,
            ti.run_id,
            scope,
        )
        raise
    result = reference.to_dict()
    LOGGER.info(
        "Source extract complete: dag_id=%s task_id=%s run_id=%s source=%s scope=%s "
        "manifest=s3://%s/%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        result["source"],
        scope,
        result["bucket"],
        result["key"],
    )
    return result


def _transform(manifest: dict[str, str]) -> dict[str, object]:
    from pipeline.etl.pse_edge import transform_to_lake
    from pipeline.storage.object_storage import get_object_store

    ti = get_current_context()["ti"]
    LOGGER.info(
        "Transformation started: dag_id=%s task_id=%s run_id=%s source=%s manifest=s3://%s/%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        manifest["source"],
        manifest["bucket"],
        manifest["key"],
    )
    try:
        result = transform_to_lake(
            manifest["bucket"],
            manifest["key"],
            object_store=get_object_store(),
        )
    except Exception:
        LOGGER.exception(
            "Transformation failed: dag_id=%s task_id=%s run_id=%s source=%s",
            ti.dag_id,
            ti.task_id,
            ti.run_id,
            manifest["source"],
        )
        raise
    LOGGER.info("Transformation complete: %s", result)
    return result


def _load(etl_manifest: dict[str, object]) -> dict[str, object]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from pipeline.etl.pse_edge import load_curated
    from pipeline.storage.object_storage import get_object_store

    ti = get_current_context()["ti"]
    LOGGER.info(
        "Sink load started: dag_id=%s task_id=%s run_id=%s source=%s manifest=s3://%s/%s "
        "warehouse_conn_id=pse_warehouse",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        etl_manifest["source"],
        etl_manifest["bucket"],
        etl_manifest["key"],
    )
    engine = PostgresHook(postgres_conn_id="pse_warehouse").get_sqlalchemy_engine(
        engine_kwargs={"pool_pre_ping": True, "pool_recycle": 1800}
    )
    try:
        result = load_curated(
            str(etl_manifest["bucket"]),
            str(etl_manifest["key"]),
            warehouse_engine=engine,
            object_store=get_object_store(),
        )
    except Exception:
        LOGGER.exception(
            "Sink load failed: dag_id=%s task_id=%s run_id=%s source=%s",
            ti.dag_id,
            ti.task_id,
            ti.run_id,
            etl_manifest["source"],
        )
        raise
    finally:
        engine.dispose()
    LOGGER.info("Sink load complete: %s", result)
    return result


@task(queue="default")
def list_configured_pse_edge_jobs() -> list[str]:
    from pipeline.sources.pse_edge.catalog import (
        DEFAULT_PSE_EDGE_JOBS,
        PSE_EDGE_FULL_BACKFILL_JOBS,
        PSE_EDGE_JOBS,
    )

    run_config = get_current_context()["dag_run"].conf or {}
    configured = (
        run_config.get("jobs")
        or os.getenv("PSE_EDGE_JOBS")
        or ",".join(DEFAULT_PSE_EDGE_JOBS)
    )
    if isinstance(configured, str):
        scopes = [item.strip() for item in configured.split(",") if item.strip()]
    elif isinstance(configured, list) and all(isinstance(item, str) for item in configured):
        scopes = [item.strip() for item in configured if item.strip()]
    else:
        raise ValueError("The jobs run configuration must be a comma-separated string or string list")
    scopes = list(dict.fromkeys(scopes))
    expanded_scopes = []
    for scope in scopes:
        if scope == "all":
            expanded_scopes.extend(PSE_EDGE_FULL_BACKFILL_JOBS)
        else:
            expanded_scopes.append(scope)
    scopes = list(dict.fromkeys(expanded_scopes))
    if not scopes:
        raise ValueError("PSE_EDGE_JOBS must contain at least one configured job")
    valid_scopes = {"configured_sources", *PSE_EDGE_JOBS}
    unknown = [scope for scope in scopes if scope not in valid_scopes]
    if unknown:
        raise ValueError(
            f"Unknown PSE_EDGE_JOBS values: {', '.join(unknown)}; "
            f"available jobs: configured_sources, {', '.join(PSE_EDGE_JOBS)}"
        )
    LOGGER.info("Configured PSE EDGE jobs: %s", scopes)
    return scopes


@task(
    queue="scrape-light",
    pool="pse_edge_source",
    pool_slots=SOURCE_JOB_SLOTS,
    retries=2,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(hours=24),
)
def capture_pse_edge_job(scope: str) -> dict[str, str]:
    run_config = get_current_context()["dag_run"].conf or {}
    selected_jobs = run_config.get("jobs") or os.getenv("PSE_EDGE_JOBS", "")
    if isinstance(selected_jobs, str):
        selected_jobs = {item.strip() for item in selected_jobs.split(",") if item.strip()}
    elif isinstance(selected_jobs, list):
        selected_jobs = {item.strip() for item in selected_jobs if isinstance(item, str)}
    else:
        selected_jobs = set()
    return _capture(
        scope=scope,
        options={
            "max_concurrency": SOURCE_JOB_SLOTS,
            "request_delay_seconds": SOURCE_JOB_REQUEST_DELAY,
            "fail_on_required_failures": True,
            "full_history": bool(run_config.get("full_history")) or "all" in selected_jobs,
        },
    )


@task(queue="transform", retries=2, retry_delay=timedelta(minutes=5))
def transform_pse_edge(manifest: dict[str, str]):
    return _transform(manifest)


@task(
    queue="transform",
    retries=2,
    retry_delay=timedelta(minutes=5),
)
def load_pse_edge(etl_manifest: dict[str, object]):
    return _load(etl_manifest)


@task(queue="default", outlets=[PSE_EDGE_WAREHOUSE])
def publish_pse_edge_warehouse(load_results: list[dict[str, object]]) -> dict[str, object]:
    """Publish one asset event after every load in an ingestion batch succeeds."""
    LOGGER.info("PSE EDGE warehouse batch published: job_results=%d", len(load_results))
    return {"jobs_loaded": len(load_results)}


@task_group(group_id="ingest_pse_edge_job")
def ingest_pse_edge_job(scope: str):
    """Run one independently retryable capture, transform, and load path."""
    manifest = capture_pse_edge_job(scope)
    transformed = transform_pse_edge(manifest)
    return load_pse_edge(transformed)


@dag(
    dag_id="pse_edge_full_ingestion",
    description="Run independently mapped PSE EDGE domain jobs and publish one warehouse update after the batch.",
    schedule="15 3 * * 1-5",
    start_date=MANILA_START,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2},
    tags=["pse", "edge", "etl"],
)
def pse_edge_full_ingestion():
    scopes = list_configured_pse_edge_jobs()
    job_results = ingest_pse_edge_job.expand(scope=scopes)
    publish_pse_edge_warehouse(job_results)


pse_edge_full_ingestion()

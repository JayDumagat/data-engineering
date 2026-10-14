"""Airflow orchestration for PSE EDGE capture and warehouse loading."""

from datetime import timedelta
import hashlib
import logging
import os

import pendulum
from airflow.sdk import dag, get_current_context, task

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


def _capture(
    scope: str,
    *,
    run_id: str | None = None,
    options: dict[str, object] | None = None,
) -> dict[str, str]:
    from pipeline.core.contracts import IngestionRequest
    from pipeline.sources.pse_edge.connector import PSEEdgeConnector

    ti = get_current_context()["ti"]
    pipeline_run_id = run_id or f"{ti.dag_id}__{ti.run_id}"
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
        reference = PSEEdgeConnector().extract(
            IngestionRequest(
                source="pse_edge",
                scope=scope,
                run_id=pipeline_run_id,
                options=options or {},
            )
        )
        if (options or {}).get("fail_on_required_failures"):
            from pipeline.etl.extract import read_manifest
            from pipeline.storage.object_storage import S3ObjectStore

            source_manifest = read_manifest(reference, S3ObjectStore())
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
        result = transform_to_lake(manifest["bucket"], manifest["key"])
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


@task(
    queue="scrape-light",
    pool="pse_edge_source",
    pool_slots=SOURCE_POOL_SLOTS,
    retries=2,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(hours=3),
)
def capture_pse_edge():
    return _capture(scope="all", options={"max_concurrency": SOURCE_POOL_SLOTS})


@task(queue="transform", retries=2, retry_delay=timedelta(minutes=5))
def transform_pse_edge(manifest: dict[str, str]):
    return _transform(manifest)


@task(
    queue="transform",
    retries=2,
    retry_delay=timedelta(minutes=5),
    outlets=[PSE_EDGE_WAREHOUSE],
)
def load_pse_edge(etl_manifest: dict[str, object]):
    return _load(etl_manifest)


@dag(
    dag_id="pse_edge_full_ingestion",
    description="Capture the PSE EDGE catalog, transform it to the lake, and load routed warehouse data.",
    schedule="15 3 * * 1-5",
    start_date=MANILA_START,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2},
    tags=["pse", "edge", "etl"],
)
def pse_edge_full_ingestion():
    manifest = capture_pse_edge()
    transformed = transform_pse_edge(manifest)
    load_pse_edge(transformed)


@task(queue="default")
def list_configured_pse_rss_feeds() -> list[str]:
    from pipeline.sources.pse_edge.scraper import configured_rss_feeds

    feeds = configured_rss_feeds()
    if not feeds:
        raise ValueError("PSE_RSS_FEEDS must contain at least one feed URL")
    LOGGER.info("RSS feed mapping prepared: feed_count=%d", len(feeds))
    return feeds


@task(
    queue="scrape-light",
    pool="pse_edge_source",
    pool_slots=1,
    retries=2,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=10),
)
def capture_pse_edge_rss_feed(feed_url: str) -> dict[str, str]:
    ti = get_current_context()["ti"]
    feed_key = hashlib.sha256(feed_url.encode("utf-8")).hexdigest()[:12]
    feed_run_id = f"{ti.dag_id}__{ti.run_id}__rss_{feed_key}"
    LOGGER.info(
        "Mapped RSS feed capture started: dag_id=%s task_id=%s run_id=%s feed_key=%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        feed_key,
    )
    return _capture(
        scope="rss",
        run_id=feed_run_id,
        options={
            "feed_urls": [feed_url],
            "max_concurrency": 1,
            "fail_on_required_failures": True,
        },
    )


@task(queue="transform", retries=2, retry_delay=timedelta(minutes=5))
def merge_pse_edge_rss_manifests(references: list[dict[str, str]]) -> dict[str, str]:
    from pipeline.sources.pse_edge.connector import merge_rss_manifests

    ti = get_current_context()["ti"]
    run_id = f"{ti.dag_id}__{ti.run_id}"
    merged = merge_rss_manifests(references, run_id=run_id)
    LOGGER.info(
        "RSS manifest reduction complete: dag_id=%s task_id=%s run_id=%s feed_manifests=%d "
        "manifest=s3://%s/%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        len(references),
        merged["bucket"],
        merged["key"],
    )
    return merged


@dag(
    dag_id="pse_edge_rss_ingestion",
    description="Map configured RSS feeds to independent captures, reduce manifests, and load routed data.",
    schedule="*/15 * * * *",
    start_date=MANILA_START,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2},
    tags=["pse", "rss", "etl"],
)
def pse_edge_rss_ingestion():
    feed_urls = list_configured_pse_rss_feeds()
    feed_manifests = capture_pse_edge_rss_feed.expand(feed_url=feed_urls)
    merged_manifest = merge_pse_edge_rss_manifests(feed_manifests)
    transformed = transform_pse_edge(merged_manifest)
    load_pse_edge(transformed)


pse_edge_full_ingestion()
pse_edge_rss_ingestion()

"""Airflow capture, transform, and load DAG for the public PSE website."""

from datetime import timedelta
import hashlib
import logging
import os

import pendulum
from airflow.sdk import dag, get_current_context, task

from pse_edge_assets import PSE_WEBSITE_WAREHOUSE


LOGGER = logging.getLogger(__name__)
MANILA_START = pendulum.datetime(2026, 1, 1, tz="Asia/Manila")


def _pool_slots() -> int:
    try:
        return max(1, min(16, int(os.getenv("PSE_WEBSITE_POOL_SLOTS", "2"))))
    except ValueError:
        LOGGER.warning("Invalid PSE_WEBSITE_POOL_SLOTS; using 2 slots")
        return 2


def _fail_if_robots_blocked(manifest: dict, bucket: str, key: str) -> None:
    if manifest.get("coverage_status") != "blocked_by_robots":
        return
    from airflow.exceptions import AirflowFailException

    raise AirflowFailException(
        "PSE robots.txt disallows every configured crawl URL; no source pages were fetched. "
        f"Diagnostics=s3://{bucket}/{key}"
    )


WEBSITE_POOL_SLOTS = _pool_slots()


@task(
    queue="scrape-browser",
    pool="pse_website_source",
    pool_slots=WEBSITE_POOL_SLOTS,
    retries=2,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(hours=4),
)
def capture_pse_website() -> dict[str, str]:
    from pipeline.core.contracts import IngestionRequest
    from pipeline.etl.extract import read_manifest
    from pipeline.sources.pse_website.connector import PSEWebsiteConnector
    from pipeline.storage.object_storage import get_object_store

    ti = get_current_context()["ti"]
    run_id = f"{ti.dag_id}__{ti.run_id}"
    LOGGER.info(
        "Source extract started: dag_id=%s task_id=%s run_id=%s source=pse_website scope=public_site workers=%d",
        ti.dag_id, ti.task_id, ti.run_id, WEBSITE_POOL_SLOTS,
    )
    try:
        object_store = get_object_store()
        reference = PSEWebsiteConnector(object_store=object_store).extract(
            IngestionRequest(
                source="pse_website",
                scope="public_site",
                run_id=run_id,
                options={"max_concurrency": WEBSITE_POOL_SLOTS},
            )
        )
        manifest = read_manifest(reference, object_store)
        required = [failure for failure in manifest.get("failures", []) if failure.get("required")]
        LOGGER.info(
            "Source capture summary: source=pse_website pages=%s assets=%s unchanged=%s failures=%d "
            "pending=%s robots_disallowed=%s coverage=%s truncated=%s",
            manifest.get("visited_pages", 0), manifest.get("visited_assets", 0),
            manifest.get("unchanged_urls", 0), len(manifest.get("failures", [])),
            manifest.get("pending_urls", 0), manifest.get("robots_disallowed_urls", 0),
            manifest.get("coverage_status", "unknown"), manifest.get("truncated", False),
        )
        _fail_if_robots_blocked(manifest, reference.bucket, reference.key)
        if required:
            raise RuntimeError(
                f"Capture recorded {len(required)} required source failures; "
                f"diagnostics=s3://{reference.bucket}/{reference.key}"
            )
    except Exception:
        LOGGER.exception(
            "Source extract failed: dag_id=%s task_id=%s run_id=%s source=pse_website scope=public_site",
            ti.dag_id, ti.task_id, ti.run_id,
        )
        raise
    result = reference.to_dict()
    LOGGER.info("Source extract complete: manifest=s3://%s/%s", result["bucket"], result["key"])
    return result


@task(queue="transform", retries=2, retry_delay=timedelta(minutes=5))
def transform_pse_website(manifest: dict[str, str]) -> dict[str, object]:
    from pipeline.etl.pipeline import transform_to_lake
    from pipeline.storage.object_storage import get_object_store

    ti = get_current_context()["ti"]
    LOGGER.info(
        "Transformation started: dag_id=%s task_id=%s run_id=%s source=pse_website manifest=s3://%s/%s",
        ti.dag_id, ti.task_id, ti.run_id, manifest["bucket"], manifest["key"],
    )
    try:
        result = transform_to_lake(
            manifest["bucket"],
            manifest["key"],
            object_store=get_object_store(),
        )
    except Exception:
        LOGGER.exception("PSE website transformation failed: dag_id=%s run_id=%s", ti.dag_id, ti.run_id)
        raise
    LOGGER.info("PSE website transformation complete: %s", result)
    return result


@task(
    queue="transform",
    retries=2,
    retry_delay=timedelta(minutes=5),
)
def load_pse_website(etl_manifest: dict[str, object]) -> dict[str, object]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from pipeline.etl.pipeline import load_curated
    from pipeline.storage.object_storage import get_object_store

    ti = get_current_context()["ti"]
    LOGGER.info(
        "Sink load started: dag_id=%s task_id=%s run_id=%s source=pse_website warehouse_conn_id=pse_warehouse",
        ti.dag_id, ti.task_id, ti.run_id,
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
        LOGGER.exception("PSE website sink load failed: dag_id=%s run_id=%s", ti.dag_id, ti.run_id)
        raise
    finally:
        engine.dispose()
    LOGGER.info("PSE website sink load complete: %s", result)
    return result


@task(queue="default", outlets=[PSE_WEBSITE_WAREHOUSE])
def publish_pse_website_warehouse(load_results: list[dict[str, object]]) -> dict[str, int]:
    LOGGER.info("PSE website warehouse batch published: feed_or_page_loads=%d", len(load_results))
    return {"loads_published": len(load_results)}


@task(queue="default")
def list_configured_pse_website_rss_feeds() -> list[str]:
    from pipeline.sources.pse_website.crawler import configured_website_rss_feeds

    feeds = configured_website_rss_feeds()
    if not feeds:
        raise ValueError("PSE_WEBSITE_RSS_FEEDS must contain at least one feed URL")
    LOGGER.info("PSE website RSS feed mapping prepared: feed_count=%d", len(feeds))
    return feeds


@task(
    queue="scrape-light",
    pool="pse_website_source",
    pool_slots=1,
    retries=2,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=10),
)
def capture_pse_website_rss_feed(feed_url: str) -> dict[str, str]:
    from pipeline.core.contracts import IngestionRequest
    from pipeline.etl.extract import read_manifest
    from pipeline.sources.pse_website.connector import PSEWebsiteConnector
    from pipeline.storage.object_storage import get_object_store

    ti = get_current_context()["ti"]
    feed_key = hashlib.sha256(feed_url.encode("utf-8")).hexdigest()[:12]
    run_id = f"{ti.dag_id}__{ti.run_id}__rss_{feed_key}__try_{ti.try_number}"
    LOGGER.info(
        "PSE website RSS capture started: dag_id=%s task_id=%s run_id=%s feed_key=%s",
        ti.dag_id, ti.task_id, ti.run_id, feed_key,
    )
    object_store = get_object_store()
    try:
        reference = PSEWebsiteConnector(object_store=object_store).extract(
            IngestionRequest(
                source="pse_website",
                scope="rss",
                run_id=run_id,
                options={"feed_urls": [feed_url], "max_concurrency": 1},
            )
        )
        manifest = read_manifest(reference, object_store)
        required = [failure for failure in manifest.get("failures", []) if failure.get("required")]
        LOGGER.info(
            "PSE website RSS capture summary: feed_key=%s objects=%d failures=%d records=%s",
            feed_key, len(manifest.get("objects", [])), len(manifest.get("failures", [])),
            sum(int(item.get("records_found", 0) or 0) for item in manifest.get("objects", [])),
        )
        _fail_if_robots_blocked(manifest, reference.bucket, reference.key)
        if required:
            raise RuntimeError(
                f"RSS capture recorded {len(required)} required source failures; "
                f"diagnostics=s3://{reference.bucket}/{reference.key}"
            )
    except Exception:
        LOGGER.exception(
            "PSE website RSS capture failed: dag_id=%s task_id=%s run_id=%s feed_key=%s",
            ti.dag_id, ti.task_id, ti.run_id, feed_key,
        )
        raise
    result = reference.to_dict()
    LOGGER.info("PSE website RSS capture complete: manifest=s3://%s/%s", result["bucket"], result["key"])
    return result


@dag(
    dag_id="pse_website_public_ingestion",
    description="Capture focused public PSE datasets, transform them to the lake, and load searchable records.",
    schedule="30 4 * * *",
    start_date=MANILA_START,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2},
    tags=["pse", "website", "etl"],
)
def pse_website_public_ingestion():
    source_manifest = capture_pse_website()
    transformed = transform_pse_website(source_manifest)
    loaded = load_pse_website(transformed)
    publish_pse_website_warehouse([loaded])


@dag(
    dag_id="pse_website_rss_ingestion",
    description="Map public PSE website RSS feeds to independent capture, transform, and load tasks.",
    schedule="*/15 * * * *",
    start_date=MANILA_START,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2},
    tags=["pse", "website", "rss", "etl"],
)
def pse_website_rss_ingestion():
    feed_urls = list_configured_pse_website_rss_feeds()
    manifests = capture_pse_website_rss_feed.expand(feed_url=feed_urls)
    transformed = transform_pse_website.expand(manifest=manifests)
    loaded = load_pse_website.expand(etl_manifest=transformed)
    publish_pse_website_warehouse(loaded)


pse_website_public_ingestion()
pse_website_rss_ingestion()

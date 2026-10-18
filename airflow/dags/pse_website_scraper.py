"""Airflow capture, transform, and load DAG for the public PSE website."""

from datetime import timedelta
import logging
import os

import pendulum
from airflow.sdk import dag, get_current_context, task

from pse_edge_assets import PSE_WEBSITE_WAREHOUSE


LOGGER = logging.getLogger(__name__)
MANILA_START = pendulum.datetime(2026, 1, 1, tz="Asia/Manila")


def _pool_slots() -> int:
    try:
        return max(1, min(16, int(os.getenv("PSE_WEBSITE_POOL_SLOTS", "4"))))
    except ValueError:
        LOGGER.warning("Invalid PSE_WEBSITE_POOL_SLOTS; using 4 slots")
        return 4


WEBSITE_POOL_SLOTS = _pool_slots()


@task(
    queue="scrape-light",
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
    outlets=[PSE_WEBSITE_WAREHOUSE],
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


@dag(
    dag_id="pse_website_public_ingestion",
    description="Capture public PSE pages and linked documents, transform to the lake, and load metadata to PostgreSQL.",
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
    load_pse_website(transformed)


pse_website_public_ingestion()

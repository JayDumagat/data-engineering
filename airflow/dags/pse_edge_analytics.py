"""Refresh PSE dbt analytics after either source warehouse changes."""

from datetime import timedelta
import logging
import os
import subprocess

import pendulum
from airflow.sdk import dag, get_current_context, task

from pse_edge_assets import PSE_EDGE_WAREHOUSE, PSE_WEBSITE_WAREHOUSE


LOGGER = logging.getLogger(__name__)
MANILA_START = pendulum.datetime(2026, 1, 1, tz="Asia/Manila")
SOURCE_MODELS = {
    "pse_edge": (
        "stg_pse_edge_records",
        "fct_pse_edge_ingestion_runs",
        "fct_pse_edge_dataset_activity",
        "fct_pse_edge_records",
    ),
    "pse_website": (
        "stg_pse_website_records",
        "fct_pse_website_ingestion_runs",
        "fct_pse_website_activity",
        "fct_pse_website_listing_statistics",
        "fct_pse_website_records",
    ),
}


@task(
    queue="transform",
    retries=1,
    retry_delay=timedelta(minutes=2),
    execution_timeout=timedelta(minutes=45),
)
def run_pse_edge_dbt() -> dict[str, str]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ti = get_current_context()["ti"]
    project_dir = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
    profiles_dir = os.getenv("DBT_PROFILES_DIR", project_dir)

    connection = PostgresHook(postgres_conn_id="pse_warehouse").get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    to_regclass('pse_edge.records') is not null
                        and to_regclass('pse_edge.ingestion_runs') is not null as pse_edge_ready,
                    to_regclass('pse_website.records') is not null
                        and to_regclass('pse_website.ingestion_runs') is not null as pse_website_ready
                """
            )
            edge_ready, website_ready = cursor.fetchone()
    finally:
        connection.close()

    ready_sources = []
    selected_models = []
    if edge_ready:
        ready_sources.append("pse_edge")
        selected_models.extend(SOURCE_MODELS["pse_edge"])
    if website_ready:
        ready_sources.append("pse_website")
        selected_models.extend(SOURCE_MODELS["pse_website"])
    if not selected_models:
        raise RuntimeError("No initialized PSE source schemas were found in the warehouse")

    LOGGER.info(
        "dbt run started: dag_id=%s task_id=%s run_id=%s project_dir=%s sources=%s models=%d",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        project_dir,
        ",".join(ready_sources),
        len(selected_models),
    )
    with subprocess.Popen(
        [
            "dbt",
            "run",
            "--project-dir",
            project_dir,
            "--profiles-dir",
            profiles_dir,
            "--select",
            *selected_models,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as process:
        if process.stdout is None:
            raise RuntimeError("dbt did not provide a task-log output stream")
        for line in process.stdout:
            LOGGER.info("dbt: %s", line.rstrip())
        return_code = process.wait()
    if return_code:
        raise RuntimeError(f"dbt run failed with exit code {return_code}")
    result = {"run_id": ti.run_id, "dbt_status": "success"}
    LOGGER.info("dbt run complete: %s", result)
    return result


@dag(
    dag_id="pse_edge_analytics",
    description="Build PSE dbt models when either source warehouse asset is updated.",
    schedule=PSE_EDGE_WAREHOUSE | PSE_WEBSITE_WAREHOUSE,
    start_date=MANILA_START,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 1},
    tags=["pse", "analytics", "dbt"],
)
def pse_edge_analytics():
    run_pse_edge_dbt()


pse_edge_analytics()

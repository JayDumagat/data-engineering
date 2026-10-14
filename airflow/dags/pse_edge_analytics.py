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


@task(
    queue="transform",
    retries=1,
    retry_delay=timedelta(minutes=2),
    execution_timeout=timedelta(minutes=45),
)
def run_pse_edge_dbt() -> dict[str, str]:
    ti = get_current_context()["ti"]
    project_dir = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
    profiles_dir = os.getenv("DBT_PROFILES_DIR", project_dir)
    LOGGER.info(
        "dbt run started: dag_id=%s task_id=%s run_id=%s project_dir=%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        project_dir,
    )
    with subprocess.Popen(
        ["dbt", "run", "--project-dir", project_dir, "--profiles-dir", profiles_dir],
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

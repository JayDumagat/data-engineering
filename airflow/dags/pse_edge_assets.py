"""Shared Airflow asset identifiers for PSE warehouse outputs."""

import os

from airflow.sdk import Asset

_WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "warehouse-postgres")
_WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5432")
_WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "warehouse")

PSE_EDGE_WAREHOUSE = Asset(
    f"postgres://{_WAREHOUSE_HOST}:{_WAREHOUSE_PORT}/{_WAREHOUSE_DB}/pse_edge/records"
)
PSE_WEBSITE_WAREHOUSE = Asset(
    f"postgres://{_WAREHOUSE_HOST}:{_WAREHOUSE_PORT}/{_WAREHOUSE_DB}/pse_website/records"
)

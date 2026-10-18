"""Keep project-owned Airflow pools and the admin user in sync."""

from __future__ import annotations

import os
import subprocess
from urllib.parse import unquote, urlsplit

import psycopg2


POOL_DESCRIPTIONS = {
    "pse_edge_source": "Shared PSE EDGE request capacity across capture DAGs",
    "pse_website_source": "PSE public website request capacity",
}


def _pool_slots(environment_key: str, default: int) -> int:
    value = os.getenv(environment_key, str(default))
    try:
        slots = int(value)
    except ValueError:
        slots = 0

    if slots < 1:
        print(f"{environment_key} must be positive; using {default}.")
        return default
    return slots


def _connect_to_metadata_database():
    database_url = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    parsed_url = urlsplit(
        database_url.replace("postgresql+psycopg2://", "postgresql://", 1)
    )
    return psycopg2.connect(
        dbname=unquote(parsed_url.path.lstrip("/")),
        user=unquote(parsed_url.username or ""),
        password=unquote(parsed_url.password or ""),
        host=parsed_url.hostname,
        port=parsed_url.port or 5432,
        connect_timeout=10,
    )


def _read_bootstrap_state(pool_names: list[str], admin_username: str):
    connection = _connect_to_metadata_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pool, slots, description FROM slot_pool WHERE pool = ANY(%s)",
                (pool_names,),
            )
            pools = {
                pool_name: (slots, description)
                for pool_name, slots, description in cursor.fetchall()
            }
            cursor.execute(
                "SELECT 1 FROM ab_user WHERE username = %s",
                (admin_username,),
            )
            admin_exists = cursor.fetchone() is not None
        return pools, admin_exists
    finally:
        connection.close()


def main() -> None:
    pools = {
        "pse_edge_source": (
            _pool_slots("PSE_SOURCE_POOL_SLOTS", 6),
            POOL_DESCRIPTIONS["pse_edge_source"],
        ),
        "pse_website_source": (
            _pool_slots("PSE_WEBSITE_POOL_SLOTS", 4),
            POOL_DESCRIPTIONS["pse_website_source"],
        ),
    }
    admin_username = os.getenv("AIRFLOW_ADMIN_USERNAME", "admin")
    current_pools, admin_exists = _read_bootstrap_state(
        list(pools), admin_username
    )

    for pool_name, (slots, description) in pools.items():
        if current_pools.get(pool_name) == (slots, description):
            print(f"Pool {pool_name} is already configured.")
            continue

        subprocess.run(
            ["airflow", "pools", "set", pool_name, str(slots), description],
            check=True,
        )

    if admin_exists:
        print(f"Admin user {admin_username} already exists.")
        return

    print(f"Creating admin user {admin_username}.")
    subprocess.run(
        [
            "airflow",
            "users",
            "create",
            "--username",
            admin_username,
            "--password",
            os.environ["AIRFLOW_ADMIN_PASSWORD"],
            "--firstname",
            os.getenv("AIRFLOW_ADMIN_FIRSTNAME", "Data"),
            "--lastname",
            os.getenv("AIRFLOW_ADMIN_LASTNAME", "Admin"),
            "--role",
            "Admin",
            "--email",
            os.getenv("AIRFLOW_ADMIN_EMAIL", "admin@example.local"),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()

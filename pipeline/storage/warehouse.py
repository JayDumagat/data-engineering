"""Warehouse schema and idempotent PostgreSQL writes for source pipelines."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from sqlalchemy import create_engine, text


_SCHEMA_NAME = re.compile(r"^[a-z][a-z0-9_]{0,39}$")


def _schema_for_source(source: str) -> str:
    schema = source.strip().lower()
    if not _SCHEMA_NAME.fullmatch(schema):
        raise ValueError(f"Invalid warehouse source/schema name: {source!r}")
    return schema


def get_warehouse_engine():
    database_url = os.environ.get("WAREHOUSE_DATABASE_URL")
    if not database_url:
        raise RuntimeError("WAREHOUSE_DATABASE_URL is required for warehouse ETL")
    return create_engine(database_url, pool_pre_ping=True, pool_recycle=1800)


def ensure_source_schema(engine, source: str) -> None:
    """Create source-isolated record and ingestion tables for a connector."""
    schema = _schema_for_source(source)
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {schema}",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.records (
            record_id TEXT PRIMARY KEY,
            dataset TEXT NOT NULL,
            record_key TEXT NOT NULL,
            source_url TEXT NOT NULL,
            raw_bucket TEXT NOT NULL,
            raw_key TEXT NOT NULL,
            payload JSONB NOT NULL,
            first_seen_at TIMESTAMPTZ NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL,
            UNIQUE (dataset, record_key)
        )
        """,
        f"CREATE INDEX IF NOT EXISTS ix_records_dataset_seen ON {schema}.records (dataset, last_seen_at DESC)",
        f"CREATE INDEX IF NOT EXISTS ix_records_payload_gin ON {schema}.records USING GIN (payload)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.ingestion_runs (
            run_id TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ NOT NULL,
            manifest_bucket TEXT NOT NULL,
            manifest_key TEXT NOT NULL,
            raw_object_count INTEGER NOT NULL,
            stage_record_count INTEGER NOT NULL,
            curated_record_count INTEGER NOT NULL,
            failures JSONB NOT NULL DEFAULT '[]'::jsonb
        )
        """,
        f"CREATE INDEX IF NOT EXISTS ix_ingestion_runs_completed ON {schema}.ingestion_runs (completed_at DESC)",
    ]
    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement)


def ensure_pse_edge_schema(engine) -> None:
    """Backward-compatible initializer for the PSE EDGE schema."""
    ensure_source_schema(engine, "pse_edge")


def _batches(records: Iterable[Mapping[str, Any]], batch_size: int) -> Iterator[list[Mapping[str, Any]]]:
    batch: list[Mapping[str, Any]] = []
    for record in records:
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def upsert_records(
    engine,
    records: Iterable[Mapping[str, Any]],
    batch_size: int = 500,
    *,
    source: str = "pse_edge",
) -> int:
    """Upsert normalized rows in bounded SQL batches without materializing all rows."""
    schema = _schema_for_source(source)
    statement = text(
        f"""
        INSERT INTO {schema}.records (
            record_id, dataset, record_key, source_url, raw_bucket, raw_key,
            payload, first_seen_at, last_seen_at
        ) VALUES (
            :record_id, :dataset, :record_key, :source_url, :raw_bucket, :raw_key,
            CAST(:payload AS JSONB), :seen_at, :seen_at
        )
        ON CONFLICT (dataset, record_key) DO UPDATE SET
            source_url = EXCLUDED.source_url,
            raw_bucket = EXCLUDED.raw_bucket,
            raw_key = EXCLUDED.raw_key,
            payload = EXCLUDED.payload,
            last_seen_at = EXCLUDED.last_seen_at
        """
    )
    written = 0
    with engine.begin() as connection:
        for batch in _batches(records, batch_size):
            params = [
                {
                    "record_id": row["record_id"],
                    "dataset": row["dataset"],
                    "record_key": row["record_key"],
                    "source_url": row["source_url"],
                    "raw_bucket": row["raw_bucket"],
                    "raw_key": row["raw_key"],
                    "payload": row.get("payload_json")
                    or json.dumps(row["payload"], ensure_ascii=False, default=str),
                    "seen_at": row["ingested_at"],
                }
                for row in batch
            ]
            connection.execute(statement, params)
            written += len(params)
    return written


def record_ingestion_run(engine, run: dict[str, Any], *, source: str = "pse_edge") -> None:
    schema = _schema_for_source(source)
    statement = text(
        f"""
        INSERT INTO {schema}.ingestion_runs (
            run_id, scope, status, started_at, completed_at, manifest_bucket,
            manifest_key, raw_object_count, stage_record_count, curated_record_count, failures
        ) VALUES (
            :run_id, :scope, :status, :started_at, :completed_at, :manifest_bucket,
            :manifest_key, :raw_object_count, :stage_record_count, :curated_record_count,
            CAST(:failures AS JSONB)
        )
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            completed_at = EXCLUDED.completed_at,
            raw_object_count = EXCLUDED.raw_object_count,
            stage_record_count = EXCLUDED.stage_record_count,
            curated_record_count = EXCLUDED.curated_record_count,
            failures = EXCLUDED.failures
        """
    )
    params = dict(run)
    params["failures"] = json.dumps(run.get("failures", []), ensure_ascii=False, default=str)
    with engine.begin() as connection:
        connection.execute(statement, params)


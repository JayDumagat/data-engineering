"""Warehouse schema and idempotent PostgreSQL writes for source pipelines."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable, Iterator, Mapping
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
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
            record_type TEXT NOT NULL DEFAULT 'unknown',
            record_key TEXT NOT NULL,
            source_url TEXT NOT NULL,
            raw_bucket TEXT NOT NULL,
            raw_key TEXT NOT NULL,
            title TEXT,
            category TEXT,
            company_name TEXT,
            symbol TEXT,
            published_at TIMESTAMPTZ,
            report_type TEXT,
            report_period TEXT,
            report_date DATE,
            uploaded_date DATE,
            detail_url TEXT,
            download_url TEXT,
            statistic_name TEXT,
            year INTEGER,
            main_value NUMERIC,
            sme_value NUMERIC,
            total_value NUMERIC,
            payload JSONB NOT NULL,
            first_seen_at TIMESTAMPTZ NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL,
            UNIQUE (dataset, record_key)
        )
        """,
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS record_type TEXT NOT NULL DEFAULT 'unknown'",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS title TEXT",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS category TEXT",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS company_name TEXT",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS symbol TEXT",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS report_type TEXT",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS report_period TEXT",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS report_date DATE",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS uploaded_date DATE",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS detail_url TEXT",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS download_url TEXT",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS statistic_name TEXT",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS year INTEGER",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS main_value NUMERIC",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS sme_value NUMERIC",
        f"ALTER TABLE {schema}.records ADD COLUMN IF NOT EXISTS total_value NUMERIC",
        f"""
        UPDATE {schema}.records
        SET record_type = COALESCE(NULLIF(payload ->> 'record_type', ''), 'unknown')
        WHERE record_type = 'unknown'
        """,
        f"CREATE INDEX IF NOT EXISTS ix_records_dataset_seen ON {schema}.records (dataset, record_type, last_seen_at DESC)",
        f"CREATE INDEX IF NOT EXISTS ix_records_symbol ON {schema}.records (symbol) WHERE symbol IS NOT NULL",
        f"CREATE INDEX IF NOT EXISTS ix_records_payload_gin ON {schema}.records USING GIN (payload)",
        f"""
        CREATE OR REPLACE FUNCTION {schema}.merge_nonempty_jsonb(existing JSONB, incoming JSONB)
        RETURNS JSONB
        LANGUAGE plpgsql
        IMMUTABLE
        AS $$
        DECLARE
            result JSONB := COALESCE(existing, '{{}}'::jsonb);
            item RECORD;
            previous_value JSONB;
        BEGIN
            IF jsonb_typeof(incoming) = 'object' THEN
                IF jsonb_typeof(result) <> 'object' THEN
                    result := '{{}}'::jsonb;
                END IF;
                FOR item IN SELECT key, value FROM jsonb_each(incoming)
                LOOP
                    previous_value := result -> item.key;
                    IF item.value = 'null'::jsonb OR item.value = '""'::jsonb THEN
                        CONTINUE;
                    END IF;
                    IF jsonb_typeof(item.value) = 'object' THEN
                        result := jsonb_set(
                            result,
                            ARRAY[item.key],
                            {schema}.merge_nonempty_jsonb(previous_value, item.value),
                            true
                        );
                    ELSIF item.value = '[]'::jsonb AND previous_value IS NOT NULL THEN
                        CONTINUE;
                    ELSE
                        result := result || jsonb_build_object(item.key, item.value);
                    END IF;
                END LOOP;
                RETURN result;
            END IF;
            IF incoming IS NULL OR incoming = 'null'::jsonb OR incoming = '""'::jsonb
               OR incoming = '{{}}'::jsonb OR incoming = '[]'::jsonb THEN
                RETURN COALESCE(existing, incoming, '{{}}'::jsonb);
            END IF;
            RETURN incoming;
        END;
        $$
        """,
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
            record_id, dataset, record_type, record_key, source_url, raw_bucket, raw_key,
            title, category, company_name, symbol, published_at, report_type, report_period, report_date,
            uploaded_date, detail_url, download_url, statistic_name, year,
            main_value, sme_value, total_value, payload, first_seen_at, last_seen_at
        ) VALUES (
            :record_id, :dataset, :record_type, :record_key, :source_url, :raw_bucket, :raw_key,
            :title, :category, :company_name, :symbol, :published_at, :report_type, :report_period, :report_date,
            :uploaded_date, :detail_url, :download_url, :statistic_name, :year,
            :main_value, :sme_value, :total_value, CAST(:payload AS JSONB), :seen_at, :seen_at
        )
        ON CONFLICT (dataset, record_key) DO UPDATE SET
            record_type = CASE WHEN EXCLUDED.record_type = 'unknown' THEN {schema}.records.record_type ELSE EXCLUDED.record_type END,
            source_url = EXCLUDED.source_url,
            raw_bucket = EXCLUDED.raw_bucket,
            raw_key = EXCLUDED.raw_key,
            title = COALESCE(EXCLUDED.title, {schema}.records.title),
            category = COALESCE(EXCLUDED.category, {schema}.records.category),
            company_name = COALESCE(EXCLUDED.company_name, {schema}.records.company_name),
            symbol = COALESCE(EXCLUDED.symbol, {schema}.records.symbol),
            published_at = COALESCE(EXCLUDED.published_at, {schema}.records.published_at),
            report_type = COALESCE(EXCLUDED.report_type, {schema}.records.report_type),
            report_period = COALESCE(EXCLUDED.report_period, {schema}.records.report_period),
            report_date = COALESCE(EXCLUDED.report_date, {schema}.records.report_date),
            uploaded_date = COALESCE(EXCLUDED.uploaded_date, {schema}.records.uploaded_date),
            detail_url = COALESCE(EXCLUDED.detail_url, {schema}.records.detail_url),
            download_url = COALESCE(EXCLUDED.download_url, {schema}.records.download_url),
            statistic_name = COALESCE(EXCLUDED.statistic_name, {schema}.records.statistic_name),
            year = COALESCE(EXCLUDED.year, {schema}.records.year),
            main_value = COALESCE(EXCLUDED.main_value, {schema}.records.main_value),
            sme_value = COALESCE(EXCLUDED.sme_value, {schema}.records.sme_value),
            total_value = COALESCE(EXCLUDED.total_value, {schema}.records.total_value),
            payload = {schema}.merge_nonempty_jsonb({schema}.records.payload, EXCLUDED.payload),
            last_seen_at = EXCLUDED.last_seen_at
        """
    )
    written = 0
    with engine.begin() as connection:
        for batch in _batches(records, batch_size):
            params = []
            for row in batch:
                payload = row.get("payload")
                if not isinstance(payload, Mapping):
                    payload = json.loads(row.get("payload_json") or "{}")
                columns = payload.get("columns") if isinstance(payload.get("columns"), Mapping) else {}
                normalized_columns = {str(key).strip().casefold(): value for key, value in columns.items()}
                data = payload.get("data") if isinstance(payload.get("data"), Mapping) else {}
                normalized_data = {str(key).strip().casefold(): value for key, value in data.items()}

                def value(*keys: str) -> Any:
                    for key in keys:
                        candidate = payload.get(key)
                        if candidate is None:
                            candidate = normalized_columns.get(key.casefold())
                        if candidate is None:
                            candidate = normalized_data.get(key.casefold())
                        if candidate is not None and str(candidate).strip():
                            return candidate
                    return None

                def numeric(candidate: Any) -> Decimal | None:
                    if candidate is None:
                        return None
                    raw = str(candidate).strip().replace(",", "")
                    if not raw or raw in {"-", "–", "—", "N/A", "n/a"}:
                        return None
                    try:
                        return Decimal(raw)
                    except InvalidOperation:
                        return None

                def parsed_date(candidate: Any) -> date | None:
                    if candidate is None:
                        return None
                    if isinstance(candidate, datetime):
                        return candidate.date()
                    if isinstance(candidate, date):
                        return candidate
                    raw = str(candidate).strip()
                    for pattern in (
                        "%Y-%m-%d",
                        "%m/%d/%Y",
                        "%B %d, %Y",
                        "%b %d, %Y",
                        "%A, %B %d, %Y",
                        "%A, %b %d, %Y",
                    ):
                        try:
                            return datetime.strptime(raw, pattern).date()
                        except ValueError:
                            continue
                    try:
                        return parsedate_to_datetime(raw).date()
                    except (TypeError, ValueError, OverflowError):
                        return None

                def parsed_timestamp(candidate: Any) -> datetime | None:
                    if candidate is None:
                        return None
                    if isinstance(candidate, datetime):
                        parsed = candidate
                    elif isinstance(candidate, date):
                        parsed = datetime.combine(candidate, datetime.min.time())
                    else:
                        raw = str(candidate).strip()
                        try:
                            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                        except ValueError:
                            try:
                                parsed = parsedate_to_datetime(raw)
                            except (TypeError, ValueError, OverflowError):
                                day = parsed_date(raw)
                                if day is None:
                                    return None
                                parsed = datetime.combine(day, datetime.min.time())
                    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed

                try:
                    year_value = int(str(value("year", "hf:tax:years")).strip()) if value("year", "hf:tax:years") is not None else None
                except ValueError:
                    year_value = None
                params.append({
                    "record_id": row["record_id"],
                    "dataset": row["dataset"],
                    "record_type": row.get("record_type") or payload.get("record_type") or "unknown",
                    "record_key": row["record_key"],
                    "source_url": row["source_url"],
                    "raw_bucket": row["raw_bucket"],
                    "raw_key": row["raw_key"],
                    "title": value("title", "name", "announcement_title", "subject"),
                    "category": value("category", "type", "hf:categories"),
                    "company_name": value("company_name", "company", "issuer", "cmpyNm"),
                    "symbol": value("symbol", "ticker", "stock_code"),
                    "published_at": parsed_timestamp(
                        value("published_at", "published", "date", "date published", "chart_date", "trade_date", "record_date")
                    ),
                    "report_type": value("report_type", "report type"),
                    "report_period": value("report_period"),
                    "report_date": parsed_date(value("report_date", "report", "report date")),
                    "uploaded_date": parsed_date(value("uploaded_date", "date uploaded", "upload date")),
                    "detail_url": value("detail_url", "link", "url"),
                    "download_url": value("download_url"),
                    "statistic_name": value("statistic_name", "table_title"),
                    "year": year_value,
                    "main_value": numeric(value("main", "main_value")),
                    "sme_value": numeric(value("sme", "sme_value")),
                    "total_value": numeric(value("total", "total_value")),
                    "payload": row.get("payload_json")
                    or json.dumps(payload, ensure_ascii=False, default=str),
                    "seen_at": row["ingested_at"],
                })
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


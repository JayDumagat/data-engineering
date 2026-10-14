"""PostgreSQL sink adapter for records routed to the relational warehouse."""

from __future__ import annotations

from typing import Any

import polars as pl

from pipeline.core.contracts import RunContext
from pipeline.storage.warehouse import ensure_source_schema, get_warehouse_engine, upsert_records


class PostgresRecordSink:
    name = "postgres"

    def __init__(self, engine=None, *, ensure_schema: bool = True) -> None:
        self._engine = engine
        self._owns_engine = engine is None
        self._ensure_schema = ensure_schema

    def write(self, records: pl.DataFrame, context: RunContext) -> dict[str, Any]:
        if records.is_empty():
            return {"records_written": 0}
        engine = self._engine or get_warehouse_engine()
        try:
            if self._ensure_schema:
                ensure_source_schema(engine, context.source)
            written = upsert_records(
                engine,
                records.iter_rows(named=True),
                source=context.source,
            )
        finally:
            if self._owns_engine:
                engine.dispose()
        return {"records_written": written}

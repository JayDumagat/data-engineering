"""Validated source and ETL manifest envelopes."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SourceManifest(BaseModel):
    model_config = ConfigDict(extra="allow")

    schema_version: int = 1
    source: str
    scope: str
    run_id: str
    started_at: datetime
    completed_at: datetime
    objects: list[dict[str, Any]] = Field(default_factory=list)
    failures: list[dict[str, Any]] = Field(default_factory=list)

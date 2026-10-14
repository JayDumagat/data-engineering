"""Stable interfaces between source connectors, ETL stages, and data sinks."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Mapping, Protocol


@dataclass(frozen=True)
class IngestionRequest:
    source: str
    scope: str
    run_id: str
    options: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ManifestRef:
    source: str
    scope: str
    run_id: str
    bucket: str
    key: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ManifestRef:
        return cls(
            source=str(value.get("source", "pse_edge")),
            scope=str(value["scope"]),
            run_id=str(value["run_id"]),
            bucket=str(value.get("bucket", value.get("manifest_bucket", "raw"))),
            key=str(value.get("key", value.get("manifest_key", ""))),
        )


@dataclass(frozen=True)
class RunContext:
    source: str
    scope: str
    run_id: str
    started_at: datetime
    completed_at: datetime
    manifest: ManifestRef


class SourceConnector(Protocol):
    """A source that writes immutable raw inputs and returns their manifest."""

    name: str

    def extract(self, request: IngestionRequest) -> ManifestRef: ...


class RawObjectReader(Protocol):
    def read(self, bucket: str, key: str) -> bytes: ...


class ObjectStore(RawObjectReader, Protocol):
    def write(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str = "application/octet-stream",
    ) -> Mapping[str, str]: ...


class Transformer(Protocol):
    def transform(
        self,
        descriptor: Mapping[str, Any],
        body: bytes | None,
        ingested_at: datetime,
    ) -> list[dict[str, Any]]: ...


class DataSink(Protocol):
    """A destination adapter such as object storage, SQL, or a NoSQL store."""

    name: str

    def write(self, records: Any, context: RunContext) -> dict[str, Any]: ...

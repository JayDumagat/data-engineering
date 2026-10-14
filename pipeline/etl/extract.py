"""Read source manifests and their referenced immutable raw objects."""

from __future__ import annotations

from typing import Any, Mapping

from pipeline.core.contracts import ManifestRef, RawObjectReader
from pipeline.core.schemas import SourceManifest


def read_manifest(reference: ManifestRef, reader: RawObjectReader) -> dict[str, Any]:
    raw = reader.read(reference.bucket, reference.key)
    manifest = SourceManifest.model_validate_json(raw)
    return manifest.model_dump(mode="json")


def read_source_object(
    descriptor: Mapping[str, Any],
    reader: RawObjectReader,
) -> bytes | None:
    """Return raw bytes, or None when a source stored structured rows inline."""
    if descriptor.get("rss_items") is not None or descriptor.get("inline_records") is not None:
        return None
    bucket = descriptor.get("raw_bucket")
    key = descriptor.get("raw_key")
    if not bucket or not key:
        raise ValueError("Raw object descriptor must include raw_bucket and raw_key")
    return reader.read(str(bucket), str(key))

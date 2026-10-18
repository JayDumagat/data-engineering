"""Stable record keys and source response normalization."""

from __future__ import annotations

from datetime import datetime
from typing import Any
import hashlib
import json
import re

from pipeline.sources.pse_edge.parser import parse_html_records, parse_json_records, parse_rss_records


DISCLOSURE_NUMBER = re.compile(r"\b(?:CR|C|DN|LN|POR|COR|CAFDN?)\s*\d{3,6}-20\d{2}\b", re.IGNORECASE)


def stable_key(dataset: str, source_url: str, payload: dict[str, Any]) -> str:
    if payload.get("record_type") == "page_snapshot":
        return source_url
    if payload.get("record_type") == "rss_item":
        return f"{source_url}#{payload.get('id') or payload.get('link') or payload.get('title', '')}"
    if payload.get("record_type") == "json_row":
        data = payload.get("data", {})
        if isinstance(data, dict):
            date_value = data.get("CHART_DATE") or data.get("date") or data.get("DATE")
            if date_value:
                return f"{source_url}#{date_value}"
    for key in ("id", "guid", "link", "url", "loc", "edge_no", "reference"):
        value = payload.get(key)
        if value:
            return str(value).strip()

    columns = payload.get("columns", {})
    if isinstance(columns, dict):
        for key, value in columns.items():
            if re.search(r"report|circular|notice|reference|document|form number", str(key), re.IGNORECASE):
                if value and str(value).strip() not in {"-", "N/A", "TBA"}:
                    return str(value).strip()
        for value in columns.values():
            match = DISCLOSURE_NUMBER.search(str(value))
            if match:
                return match.group(0).replace(" ", "").upper()
        for key, value in columns.items():
            if dataset == "company_directory" and re.search(r"symbol|ticker", str(key), re.IGNORECASE) and value:
                return str(value).strip().upper()

    for link in payload.get("links", []):
        address = link.get("href") or ""
        match = re.search(r"edge_no=([^&#]+)", address, re.IGNORECASE)
        if match:
            return f"edge_no={match.group(1)}"
        onclick = link.get("onclick") or ""
        match = re.search(r"openDiscViewer\(['\"]?([A-Za-z0-9_-]+)", onclick)
        if match:
            return f"edge_no={match.group(1)}"
        match = re.search(r"openPopup\(['\"]([^'\"]+)", onclick)
        if match:
            return f"calendar_event={match.group(1)}"
        ids = re.findall(r"['\"]?(\d{1,12})['\"]?", onclick)
        if len(ids) >= 2:
            return ":".join(ids[:2])

    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(f"{dataset}\0{source_url}\0{canonical}".encode("utf-8")).hexdigest()


def normalize_payload(
    descriptor: dict[str, Any], payload: dict[str, Any], ingested_at: datetime
) -> dict[str, Any]:
    dataset = descriptor["dataset"]
    source_url = descriptor["url"]
    record_key = stable_key(dataset, source_url, payload)
    record_id = hashlib.sha256(f"{dataset}\0{record_key}".encode("utf-8")).hexdigest()
    return {
        "record_id": record_id,
        "dataset": dataset,
        "record_key": record_key,
        "source_url": source_url,
        "raw_bucket": descriptor["raw_bucket"],
        "raw_key": descriptor["raw_key"],
        "payload": payload,
        "ingested_at": ingested_at,
    }


def normalize_object(descriptor: dict[str, Any], body: bytes, ingested_at: datetime) -> list[dict[str, Any]]:
    dataset = descriptor["dataset"]
    source_url = descriptor["url"]
    content_type = descriptor.get("content_type", "").lower()
    if dataset == "rss" or any(kind in content_type for kind in ("xml", "rss", "atom")):
        parsed = parse_rss_records(body, source_url)
    elif "json" in content_type or body.lstrip().startswith((b"{", b"[")):
        parsed = parse_json_records(body, dataset, source_url)
    elif "html" in content_type or body.lstrip().startswith(b"<"):
        text = body.decode("utf-8", errors="replace")
        parsed, _ = parse_html_records(text, dataset, source_url)
    else:
        if content_type.startswith("image/"):
            record_type = "image_asset"
        elif content_type.startswith("video/"):
            record_type = "video_asset"
        else:
            record_type = "binary_asset"
        parsed = [
            {
                "record_type": record_type,
                "content_type": descriptor.get("content_type", "application/octet-stream"),
                "size_bytes": len(body),
            }
        ]
    return [normalize_payload(descriptor, payload, ingested_at) for payload in parsed]


def normalize_rss_items(
    descriptor: dict[str, Any], items: list[dict[str, Any]], ingested_at: datetime
) -> list[dict[str, Any]]:
    return [normalize_payload(descriptor, item, ingested_at) for item in items]

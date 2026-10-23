"""Stable record keys and source response normalization."""

from __future__ import annotations

from datetime import datetime
from typing import Any
import hashlib
import json
import logging
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pipeline.sources.pse_edge.parser import parse_html_records, parse_json_records, parse_rss_records
from pipeline.etl.pse_edge.record_quality import prepare_record


DISCLOSURE_NUMBER = re.compile(r"\b(?:CR|C|DN|LN|POR|COR|CAFDN?)\s*\d{3,6}-20\d{2}\b", re.IGNORECASE)
LOGGER = logging.getLogger(__name__)


def _canonical_url(value: Any) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    parsed = urlsplit(url)
    query = sorted(
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.casefold().startswith("utm_") and key.casefold() not in {"fbclid", "gclid"}
    )
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, urlencode(query), ""))


def _clean_payload(value: Any) -> Any:
    """Remove absent values while retaining meaningful zeroes and source fields."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        cleaned = {
            str(key): normalized
            for key, item in value.items()
            if (normalized := _clean_payload(item)) is not None
        }
        return cleaned or None
    if isinstance(value, (list, tuple)):
        cleaned = [normalized for item in value if (normalized := _clean_payload(item)) is not None]
        return cleaned or None
    return value


def _has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        return any(_has_content(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_has_content(item) for item in value)
    return True


def _meaningful_payload(payload: dict[str, Any]) -> bool:
    """Reject parser placeholders and completely empty rows before they reach sinks."""
    record_type = payload.get("record_type")
    if record_type == "page_snapshot":
        return bool(
            payload.get("title")
            or payload.get("page_text")
            or payload.get("form_fields")
            or payload.get("table_count")
        )
    if record_type == "json_snapshot":
        return bool(payload.get("metadata") or payload.get("row_count"))
    if record_type == "json_row":
        return _has_content(payload.get("data"))
    if record_type in {"table_row", "source_table_row"}:
        return _has_content(payload.get("columns")) or _has_content(payload.get("cells"))
    if record_type == "rss_item":
        return any(_has_content(payload.get(key)) for key in ("title", "link", "id", "published_at", "description"))
    return _has_content({key: value for key, value in payload.items() if key != "record_type"})


def stable_key(
    dataset: str,
    source_url: str,
    payload: dict[str, Any],
    request_params: dict[str, Any] | None = None,
) -> str:
    if dataset == "website_feeds" and payload.get("record_type") == "rss_item":
        link = _canonical_url(payload.get("link"))
        if link:
            return link
        return f"{payload.get('feed_url') or source_url}#{payload.get('id') or payload.get('title', '')}"
    if dataset == "market_reports":
        columns = payload.get("columns", {})
        if isinstance(columns, dict):
            normalized = {str(key).strip().casefold(): value for key, value in columns.items()}
            report_date = normalized.get("report") or normalized.get("report date")
            report_type = normalized.get("report type") or payload.get("title")
            if report_date and report_type:
                return f"market-report:{str(report_date).strip().casefold()}:{str(report_type).strip().casefold()}"
        download_url = payload.get("download_url")
        if download_url:
            return _canonical_url(download_url)
    if dataset in {"announcements", "press_releases"}:
        detail_url = payload.get("detail_url")
        if detail_url:
            return _canonical_url(detail_url)
        title = payload.get("title")
        event_date = payload.get("published_at")
        if title and event_date:
            return f"{str(event_date).strip().casefold()}:{str(title).strip().casefold()}"
    if dataset == "listing_statistics":
        statistic_name = payload.get("statistic_name") or payload.get("table_title")
        statistic_year = payload.get("year")
        if statistic_name and statistic_year:
            return f"{str(statistic_name).strip().casefold()}:{statistic_year}"
    if payload.get("record_type") == "page_snapshot":
        return source_url
    if payload.get("record_type") == "rss_item":
        return f"{source_url}#{payload.get('id') or payload.get('link') or payload.get('title', '')}"
    if payload.get("record_type") == "json_row":
        data = payload.get("data", {})
        if isinstance(data, dict):
            date_value = data.get("CHART_DATE") or data.get("date") or data.get("DATE")
            if date_value:
                identity_fields = {
                    "symbol",
                    "ticker",
                    "stockcode",
                    "securityid",
                    "cmpyid",
                    "companyid",
                    "indexcode",
                }
                request_scope = {
                    re.sub(r"[^a-z0-9]", "", str(key).casefold()): value
                    for key, value in (request_params or {}).items()
                    if re.sub(r"[^a-z0-9]", "", str(key).casefold()) in identity_fields
                    and value is not None
                    and str(value).strip()
                }
                row_scope = {
                    re.sub(r"[^a-z0-9]", "", str(key).casefold()): value
                    for key, value in data.items()
                    if re.sub(r"[^a-z0-9]", "", str(key).casefold()) in identity_fields
                    and value is not None
                    and str(value).strip()
                }
                identity = {"date": date_value, **request_scope, **row_scope}
                if len(identity) > 1:
                    return f"{source_url}#{json.dumps(identity, sort_keys=True, separators=(',', ':'))}"
                canonical = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
                digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                return f"{source_url}#{date_value}:{digest}"
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

    if payload.get("record_type") == "table_row":
        identity_payload = {
            "table_title": payload.get("table_title"),
            "columns": payload.get("columns", {}),
            "links": sorted(
                _canonical_url(link.get("href"))
                for link in payload.get("links", [])
                if isinstance(link, dict) and link.get("href")
            ),
        }
    elif payload.get("record_type") == "json_row":
        identity_payload = payload.get("data", {})
    else:
        identity_payload = {
            key: value
            for key, value in payload.items()
            if key not in {"row_no", "sequence", "ingested_at"}
        }
    canonical = json.dumps(identity_payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(f"{dataset}\0{source_url}\0{canonical}".encode("utf-8")).hexdigest()


def normalize_payload(
    descriptor: dict[str, Any], payload: dict[str, Any], ingested_at: datetime
) -> dict[str, Any]:
    dataset = descriptor["dataset"]
    source_url = descriptor["url"]
    payload = _clean_payload(payload) or {}
    if not _meaningful_payload(payload):
        raise ValueError(f"Refusing empty {dataset!r} source record")
    request_params = descriptor.get("request_params")
    record_key = stable_key(
        dataset,
        source_url,
        payload,
        request_params if isinstance(request_params, dict) else None,
    )
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
    records: list[dict[str, Any]] = []
    empty_state_rows = 0
    for payload in parsed:
        prepared = prepare_record(dataset, payload)
        if prepared is None:
            empty_state_rows += 1
            continue
        cleaned = _clean_payload(prepared) or {}
        if _meaningful_payload(cleaned):
            records.append(normalize_payload(descriptor, cleaned, ingested_at))
    if empty_state_rows:
        LOGGER.info(
            "Filtered empty-state PSE EDGE rows: dataset=%s rows=%d source_url=%s",
            dataset,
            empty_state_rows,
            source_url,
        )
    return records


def normalize_rss_items(
    descriptor: dict[str, Any], items: list[dict[str, Any]], ingested_at: datetime
) -> list[dict[str, Any]]:
    return [
        normalize_payload(descriptor, item, ingested_at)
        for item in items
        if _meaningful_payload(_clean_payload(item) or {})
    ]

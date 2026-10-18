"""Paced raw ingestion for the public PSE EDGE portal and configured RSS feeds."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from html import unescape
from mimetypes import guess_extension, guess_type
from typing import Any
from urllib.parse import urlencode, urlparse
from zoneinfo import ZoneInfo
import json
import logging
import os
import re
import threading
import uuid

from pipeline.core.contracts import ObjectStore
from pipeline.sources.pse_edge.catalog import (
    CALENDAR_START_YEAR,
    COMPANY_DIVIDENDS_SOURCE,
    COMPANY_DETAIL_SOURCES,
    DOCUMENT_PATHS,
    PAGE_SOURCES,
    PRICE_HISTORY_SOURCE,
    PSE_EDGE_CONFIGURED_SOURCES_JOB,
    PSE_EDGE_JOBS,
    PSE_EDGE_SCOPE_ALIASES,
    SEARCH_SOURCES,
    SearchSource,
)
from pipeline.sources.pse_edge.client import PSEEdgeClient
from pipeline.sources.pse_edge.parser import parse_html_records, parse_json_records
from pipeline.storage.object_storage import get_object_store


LOGGER = logging.getLogger(__name__)
BASE_URL = os.getenv("PSE_EDGE_BASE_URL", "https://edge.pse.com.ph").rstrip("/")
_APPEND_LOCK = threading.Lock()


def _env_limit(name: str, default: int = 0) -> int | None:
    """Read a positive safety cap; zero and negative values mean unbounded."""
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default if default > 0 else None
    return value if value > 0 else None


def _safe_segment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value).strip("_") or "run"


def _body(response: Any) -> bytes:
    value = getattr(response, "body", b"")
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8", errors="replace")


def _decode_body(body: bytes, response: Any) -> str:
    encoding = getattr(response, "encoding", None) or "utf-8"
    try:
        return body.decode(encoding, errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


def _content_type(response: Any, url: str) -> str:
    headers = getattr(response, "headers", {}) or {}
    try:
        content_type = headers.get("content-type") or headers.get("Content-Type")
    except AttributeError:
        content_type = None
    if content_type:
        return str(content_type)
    path = urlparse(url).path
    guessed_type, _ = guess_type(path)
    if guessed_type:
        return guessed_type
    path = path.lower()
    if path.endswith(".pdf") or "pdf" in path.lower():
        return "application/pdf"
    if path.endswith((".xml", "/feed/")):
        return "application/rss+xml"
    return "text/html; charset=utf-8"


def _extension(content_type: str, url: str) -> str:
    lowered = content_type.lower()
    if "pdf" in lowered or urlparse(url).path.lower().endswith(".pdf"):
        return "pdf"
    if "xml" in lowered or "rss" in lowered or "atom" in lowered:
        return "xml"
    if "json" in lowered:
        return "json"
    mime_type = lowered.split(";", 1)[0].strip()
    guessed_extension = guess_extension(mime_type) if mime_type else None
    if guessed_extension:
        return guessed_extension.lstrip(".")
    suffix = os.path.splitext(urlparse(url).path)[1].lower().lstrip(".")
    if suffix:
        return suffix
    if lowered.startswith("text/html") or not lowered:
        return "html"
    return "bin"


def _page_count_hint(html: str) -> int | None:
    clean = unescape(re.sub(r"<[^>]+>", " ", html))
    match = re.search(r"\[\s*\d+\s*/\s*(\d+)\s*\]", clean)
    return int(match.group(1)) if match else None


def _row_count(records: list[dict[str, Any]]) -> int:
    return sum(record.get("record_type") in {"table_row", "json_row"} for record in records)


def _company_ids(records: list[dict[str, Any]]) -> list[str]:
    company_ids: set[str] = set()
    for record in records:
        for field in record.get("form_fields", []):
            if field.get("name", "").lower() in {"cmpy_id", "companyid", "company_id"}:
                if str(field.get("value", "")).isdigit():
                    company_ids.add(str(field["value"]))
        for link in record.get("links", []):
            if str(link.get("company_id", "")).isdigit():
                company_ids.add(str(link["company_id"]))
            addresses = (link.get("href", ""), link.get("onclick", ""))
            for address in addresses:
                match = re.search(r"(?:cmpy_id|cmpyId|companyId)[=:'\"]+\s*(\d+)", address)
                if match:
                    company_ids.add(match.group(1))
                match = re.search(r"\(\s*['\"]?(\d+)['\"]?\s*,", address)
                if match:
                    company_ids.add(match.group(1))
    return sorted(company_ids)


def _document_url(url: str) -> bool:
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    if hostname != "pse.com.ph" and not hostname.endswith(".pse.com.ph"):
        return False
    path = parsed.path.lower()
    asset_extensions = {
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip",
        ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp",
        ".mp4", ".mov", ".webm",
    }
    return (
        any(marker.lower() in path for marker in DOCUMENT_PATHS)
        or path.endswith(tuple(asset_extensions))
    )


def _raw_key(dataset: str, ingested_at: datetime, run_id: str, sequence: int, extension: str) -> str:
    return (
        f"pse_edge/{dataset}/ingest_date={ingested_at:%Y-%m-%d}/"
        f"run_id={_safe_segment(run_id)}/part-{sequence:06d}.{extension}"
    )


def _store_raw(
    *,
    object_store: ObjectStore,
    dataset: str,
    url: str,
    response: Any,
    ingested_at: datetime,
    run_id: str,
    sequence: int,
) -> dict[str, Any]:
    payload = _body(response)
    content_type = _content_type(response, url)
    key = _raw_key(dataset, ingested_at, run_id, sequence, _extension(content_type, url))
    bucket = os.getenv("S3_BUCKET_RAW", "raw")
    object_store.write(bucket, key, payload, content_type=content_type)
    return {
        "dataset": dataset,
        "sequence": sequence,
        "url": url,
        "raw_bucket": bucket,
        "raw_key": key,
        "content_type": content_type,
        "status": int(getattr(response, "status", 200) or 200),
        "size_bytes": len(payload),
    }


def _json_url(path: str, params: dict[str, Any] | None = None) -> str:
    url = BASE_URL + path
    if params:
        url += "?" + urlencode(params)
    return url


def _fetch_and_store_page(
    fetcher: PSEEdgeClient,
    object_store: ObjectStore,
    *,
    dataset: str,
    url: str,
    method: str,
    post_form: dict[str, Any] | None = None,
    post_json: dict[str, Any] | None = None,
    request_params: dict[str, Any] | None = None,
    ingested_at: datetime,
    run_id: str,
    sequence: int,
    required: bool,
    objects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], str]:
    try:
        if method == "GET":
            response = fetcher.get(url)
        else:
            response = fetcher.post(url, form_body=post_form, json_body=post_json)
        descriptor = _store_raw(
            object_store=object_store,
            dataset=dataset,
            url=url,
            response=response,
            ingested_at=ingested_at,
            run_id=run_id,
            sequence=sequence,
        )
        if request_params is not None:
            descriptor["request_params"] = request_params
        elif post_form is not None:
            descriptor["request_params"] = post_form
        elif post_json is not None:
            descriptor["request_params"] = post_json
        with _APPEND_LOCK:
            objects.append(descriptor)
        body = _body(response)
        text = _decode_body(body, response)
        content_type = descriptor["content_type"].lower()
        if "json" in content_type or text.lstrip().startswith(("{", "[")):
            records = parse_json_records(body, dataset, url)
            links = []
        elif "html" in content_type or "xml" in content_type or text.lstrip().startswith("<"):
            lowered_text = text[:20_000].lower()
            if any(marker in lowered_text for marker in ("just a moment", "cf-chl-", "captcha", "access denied")):
                raise RuntimeError("PSE returned an anti-bot or access-denied page")
            records, links = parse_html_records(text, dataset, url)
        else:
            records, links = ([{"record_type": "binary_asset", "content_type": content_type}], [])
        descriptor["records_found"] = _row_count(records)
        return records, links, text
    except Exception as exc:
        failure = {"dataset": dataset, "url": url, "error": str(exc), "required": required}
        failure["sequence"] = sequence
        with _APPEND_LOCK:
            failures.append(failure)
        LOGGER.exception("PSE EDGE fetch failed for %s", url)
        return [], [], ""


def _fetch_many(
    fetcher: PSEEdgeClient,
    requests: list[dict[str, Any]],
) -> list[tuple[list[dict[str, Any]], list[str], str]]:
    return fetcher.map(
        lambda request: _fetch_and_store_page(fetcher, **request),
        requests,
    )


def _fetch_search_pages(
    fetcher: PSEEdgeClient,
    object_store: ObjectStore,
    *,
    dataset: str,
    source: SearchSource,
    ingested_at: datetime,
    run_id: str,
    objects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    sequence_start: int,
    date_window: tuple[str, str] | None = None,
) -> tuple[int, list[str], list[tuple[str, str]], list[str]]:
    page_limit = _env_limit("PSE_MAX_SEARCH_PAGES")
    row_total = 0
    company_ids: set[str] = set()
    security_pairs: set[tuple[str, str]] = set()
    document_urls: set[str] = set()
    seen_pages: set[str] = set()
    page_no = 1
    completed = False

    while page_limit is None or page_no <= page_limit:
        params = source.fields_for_page(page_no, date_window=date_window)
        url = _json_url(source.path, dict(source.query_params) or None)
        records, links, html = _fetch_and_store_page(
            fetcher,
            object_store,
            dataset=dataset,
            url=url,
            method="POST",
            post_form=params,
            ingested_at=ingested_at,
            run_id=run_id,
            sequence=sequence_start + page_no,
            request_params=params,
            required=True,
            objects=objects,
            failures=failures,
        )
        request_sequence = sequence_start + page_no
        if any(int(item.get("sequence", -1)) == request_sequence and item.get("dataset") == dataset for item in failures):
            break
        row_count = _row_count(records)
        if row_count == 0:
            is_json = html.lstrip().startswith(("{", "["))
            no_rows_message = any(marker in html.lower() for marker in ("no data", "no records", "no result"))
            if page_no == 1 and not is_json and not no_rows_message:
                with _APPEND_LOCK:
                    failures.append(
                        {
                            "dataset": dataset,
                            "url": url,
                            "error": "PSE search response contained no data rows; check the endpoint or request fields",
                            "required": True,
                            "sequence": request_sequence,
                        }
                    )
            completed = True
            break

        fingerprint = hashlib.sha256(html.encode("utf-8", errors="replace")).hexdigest()
        if fingerprint in seen_pages:
            failures.append(
                {
                    "dataset": dataset,
                    "url": url,
                    "error": f"PSE repeated a prior page at page {page_no}; pagination stopped to avoid an infinite loop",
                    "required": True,
                    "sequence": request_sequence,
                }
            )
            break
        seen_pages.add(fingerprint)
        row_total += row_count
        company_ids.update(_company_ids(records))
        security_pairs.update(_security_pairs(records))
        document_urls.update(link for link in links if _document_url(link))
        last_page = _page_count_hint(html)
        if last_page is not None and page_no >= last_page:
            completed = True
            break
        page_no += 1

    if page_limit is not None and page_no > page_limit and not completed:
        failures.append(
            {
                "dataset": dataset,
                "url": _json_url(source.path, dict(source.query_params) or None),
                "error": f"Search truncated at configured PSE_MAX_SEARCH_PAGES={page_limit}",
                "required": True,
                "sequence": sequence_start + page_no - 1,
                "truncated": True,
            }
        )
    return row_total, sorted(company_ids), sorted(security_pairs), sorted(document_urls)


def _parse_feed_response(body: bytes, feed_url: str) -> list[dict[str, Any]]:
    from pipeline.sources.pse_edge.parser import parse_rss_records

    return parse_rss_records(body, feed_url)


def _fetch_rss_feed(
    fetcher: PSEEdgeClient,
    object_store: ObjectStore,
    *,
    feed_url: str,
    feed_no: int,
    ingested_at: datetime,
    run_id: str,
    required: bool,
    objects: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    descriptor = None
    try:
        response = fetcher.get(feed_url)
        descriptor = _store_raw(
            object_store=object_store,
            dataset="rss",
            url=feed_url,
            response=response,
            ingested_at=ingested_at,
            run_id=run_id,
            sequence=20_000 + feed_no,
        )
        with _APPEND_LOCK:
            objects.append(descriptor)
        body = _body(response)
        feed_records = _parse_feed_response(body, feed_url)
        descriptor["records_found"] = len(feed_records)
        descriptor["rss_items"] = feed_records
        return descriptor, None
    except Exception as exc:
        LOGGER.exception("RSS fetch failed for %s", feed_url)
        failure = {
            "dataset": "rss",
            "url": feed_url,
            "error": str(exc),
            "required": required,
            "sequence": 20_000 + feed_no,
        }
        if descriptor:
            failure["raw_bucket"] = descriptor["raw_bucket"]
            failure["raw_key"] = descriptor["raw_key"]
        return descriptor, failure


def _security_pairs(records: list[dict[str, Any]]) -> list[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for record in records:
        fields = record.get("form_fields", [])
        company_ids = {
            str(field.get("value", ""))
            for field in fields
            if field.get("name", "").lower() in {"cmpy_id", "companyid", "company_id"}
            and str(field.get("value", "")).isdigit()
        }
        security_ids = {
            str(field.get("value", ""))
            for field in fields
            if field.get("name", "").lower() in {"security_id", "securityid"}
            and str(field.get("value", "")).isdigit()
        }
        pairs.update((company_id, security_id) for company_id in company_ids for security_id in security_ids)
        for link in record.get("links", []):
            onclick = link.get("onclick", "")
            if re.search(r"\bcmDetail\s*\(", onclick):
                arguments = re.findall(r"['\"]?(\d+)['\"]?", onclick)
                if len(arguments) >= 2:
                    pairs.add((arguments[0], arguments[1]))
            address = f"{link.get('href', '')} {onclick}"
            company_match = re.search(r"(?:cmpy_id|cmpyId|companyId)[=:'\"]+\s*(\d+)", address)
            security_match = re.search(r"(?:security_id|securityId)[=:'\"]+\s*(\d+)", address)
            if company_match and security_match:
                pairs.add((company_match.group(1), security_match.group(1)))
    return sorted(pairs)


def _price_history_dates() -> tuple[str, str]:
    start_date = datetime.strptime(os.getenv("PSE_PRICE_HISTORY_START_DATE", "2000-01-01"), "%Y-%m-%d")
    end_date = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Manila"))
    return start_date.strftime("%m-%d-%Y"), end_date.strftime("%m-%d-%Y")


def _calendar_months() -> list[tuple[int, int]]:
    """Enumerate calendar months from the oldest year offered by PSE EDGE."""
    try:
        start_year = int(os.getenv("PSE_CALENDAR_START_YEAR", str(CALENDAR_START_YEAR)))
    except ValueError:
        start_year = CALENDAR_START_YEAR
    current = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Manila"))
    return [
        (year, month)
        for year in range(start_year, current.year + 1)
        for month in range(1, (current.month if year == current.year else 12) + 1)
    ]


def _search_date_window(
    scope: str,
    job: Any,
    *,
    full_history: bool = False,
) -> tuple[int | None, tuple[str, str] | None]:
    """Use a small overlap for scheduled searches; the full scope stays unbounded."""
    date_filter_available = any(
        "fromDate" in dict(SEARCH_SOURCES[dataset].form_fields)
        or "toDate" in dict(SEARCH_SOURCES[dataset].form_fields)
        for dataset in job.search_datasets
    )
    if not date_filter_available:
        return None, None
    if scope == "configured_sources" or full_history:
        return None, None
    raw_override = os.getenv("PSE_EDGE_SEARCH_WINDOW_DAYS", "").strip() or None
    if raw_override is None:
        days = job.search_window_days
    else:
        try:
            configured = int(raw_override)
            days = configured if configured > 0 else None
        except ValueError:
            LOGGER.warning("Invalid PSE_EDGE_SEARCH_WINDOW_DAYS=%r; using job defaults", raw_override)
            days = job.search_window_days
    if days is None or days <= 0:
        return None, None
    today = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Manila")).date()
    start = today - timedelta(days=days - 1)
    return days, (start.strftime("%m-%d-%Y"), today.strftime("%m-%d-%Y"))


def _capture_calendar_history(
    fetcher: PSEEdgeClient,
    *,
    object_store: ObjectStore,
    ingested_at: datetime,
    run_id: str,
    sequence_start: int,
    objects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    document_urls: set[str],
) -> tuple[int, int]:
    """Capture every monthly calendar view available since PSE EDGE started."""
    sequence = _last_sequence(objects, sequence_start)
    months = _calendar_months()
    month_limit = _env_limit("PSE_MAX_CALENDAR_MONTHS")
    if month_limit is not None and len(months) > month_limit:
        failures.append(
            {
                "dataset": "market_calendar_history",
                "url": BASE_URL + PAGE_SOURCES["market_calendar"],
                "error": f"Calendar archive truncated at configured PSE_MAX_CALENDAR_MONTHS={month_limit}",
                "required": True,
                "sequence": sequence + 1,
                "truncated": True,
            }
        )
        months = months[:month_limit]

    record_count = 0
    batch_size = max(1, fetcher.max_workers * 4)
    for batch_start in range(0, len(months), batch_size):
        requests = []
        for year, month in months[batch_start : batch_start + batch_size]:
            sequence += 1
            requests.append(
                {
                    "dataset": "market_calendar_history",
                    "object_store": object_store,
                    "url": _json_url(PAGE_SOURCES["market_calendar"], {"year": year, "month": month}),
                    "method": "GET",
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "sequence": sequence,
                    "required": False,
                    "objects": objects,
                    "failures": failures,
                }
            )
        for records, links, _ in _fetch_many(fetcher, requests):
            record_count += _row_count(records)
            document_urls.update(link for link in links if _document_url(link))
    return record_count, sequence


def configured_rss_feeds() -> list[str]:
    """Return the ordered, deduplicated RSS/Atom feed URLs from configuration."""
    feeds = [
        item.strip()
        for item in os.getenv("PSE_RSS_FEEDS", "https://www.pse.com.ph/feed/").split(",")
        if item.strip()
    ]
    return list(dict.fromkeys(feeds))


def scrape(
    scope: str = "configured_sources",
    run_id: str | None = None,
    *,
    feed_urls: list[str] | None = None,
    max_concurrency: int | None = None,
    request_delay_seconds: float | None = None,
    full_history: bool = False,
    object_store: ObjectStore | None = None,
) -> dict[str, str]:
    """Capture one configured job, all configured sources, or configured RSS feeds.

    Named catalog jobs isolate related datasets. ``configured_sources`` runs
    the complete local public-source catalog, issuer expansion, history, and
    document traversal. ``all`` remains a legacy alias; ``rss`` is a separate
    PSE website feed lane. Raw response bytes are persisted before transformation.
    """
    scope = PSE_EDGE_SCOPE_ALIASES.get(scope, scope)
    if scope not in {"configured_sources", "rss", *PSE_EDGE_JOBS}:
        supported = ", ".join(
            ("configured_sources", "all (legacy alias)", "rss", *PSE_EDGE_JOBS)
        )
        raise ValueError(f"Unknown PSE EDGE job {scope!r}; supported jobs: {supported}")
    source_run_id = run_id or uuid.uuid4().hex
    raw_store = object_store if object_store is not None else get_object_store()
    with PSEEdgeClient(
        worker_limit=max_concurrency,
        request_delay_seconds=request_delay_seconds,
    ) as fetcher:
        return _scrape_with_client(
            scope,
            source_run_id,
            fetcher,
            object_store=raw_store,
            feed_urls=feed_urls,
            full_history=full_history,
        )


def _last_sequence(objects: list[dict[str, Any]], minimum: int) -> int:
    return max(
        minimum,
        max((int(item.get("sequence", 0)) for item in objects), default=0),
    )


def _capture_company_details(
    fetcher: PSEEdgeClient,
    *,
    object_store: ObjectStore,
    company_ids: set[str],
    ingested_at: datetime,
    run_id: str,
    sequence_start: int,
    objects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    document_urls: set[str],
    security_pairs: set[tuple[str, str]],
) -> tuple[int, int]:
    """Capture every public company tab and its dividends/rights result lists."""
    sequence = _last_sequence(objects, sequence_start)
    company_limit = _env_limit("PSE_MAX_COMPANY_PROFILES")
    selected_company_ids = sorted(company_ids)
    if company_limit is not None and len(selected_company_ids) > company_limit:
        failures.append(
            {
                "dataset": "company_profiles",
                "url": BASE_URL + PAGE_SOURCES["company_directory_page"],
                "error": f"Company profiles truncated at configured PSE_MAX_COMPANY_PROFILES={company_limit}",
                "required": True,
                "sequence": sequence + 1,
                "truncated": True,
            }
        )
        selected_company_ids = selected_company_ids[:company_limit]

    record_count = 0
    company_batch_size = max(1, fetcher.max_workers * 4)
    for batch_start in range(0, len(selected_company_ids), company_batch_size):
        requests: list[dict[str, Any]] = []
        for company_id in selected_company_ids[batch_start : batch_start + company_batch_size]:
            query = {"cmpy_id": company_id}
            for dataset, path in COMPANY_DETAIL_SOURCES.items():
                sequence += 1
                requests.append(
                    {
                        "dataset": dataset,
                        "object_store": object_store,
                        "url": _json_url(path, query),
                        "method": "GET",
                        "ingested_at": ingested_at,
                        "run_id": run_id,
                        "sequence": sequence,
                        "required": False,
                        "objects": objects,
                        "failures": failures,
                    }
                )

            for value in ("Dividends", "Rights"):
                sequence += 1
                url = _json_url(COMPANY_DIVIDENDS_SOURCE, {"DividendsOrRights": value})
                form = {"cmpy_id": company_id}
                requests.append(
                    {
                        "dataset": f"company_{value.lower()}",
                        "object_store": object_store,
                        "url": url,
                        "method": "POST",
                        "post_form": form,
                        "request_params": form,
                        "ingested_at": ingested_at,
                        "run_id": run_id,
                        "sequence": sequence,
                        "required": False,
                        "objects": objects,
                        "failures": failures,
                    }
                )

        for records, links, _ in _fetch_many(fetcher, requests):
            record_count += _row_count(records)
            security_pairs.update(_security_pairs(records))
            document_urls.update(link for link in links if _document_url(link))
    return record_count, sequence


def _capture_price_history(
    fetcher: PSEEdgeClient,
    *,
    object_store: ObjectStore,
    security_pairs: set[tuple[str, str]],
    ingested_at: datetime,
    run_id: str,
    sequence_start: int,
    objects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> tuple[int, int]:
    """Capture price histories for every discovered company/security pair."""
    sequence = _last_sequence(objects, sequence_start)
    start_date, end_date = _price_history_dates()
    pair_limit = _env_limit("PSE_MAX_PRICE_HISTORIES_PER_RUN")
    all_pairs = sorted(security_pairs)
    if pair_limit is not None and len(all_pairs) > pair_limit:
        failures.append(
            {
                "dataset": "price_history",
                "url": BASE_URL + PRICE_HISTORY_SOURCE,
                "error": f"Price histories truncated at configured PSE_MAX_PRICE_HISTORIES_PER_RUN={pair_limit}",
                "required": True,
                "sequence": sequence + 1,
                "truncated": True,
            }
        )
        all_pairs = all_pairs[:pair_limit]

    record_count = 0
    batch_size = max(1, fetcher.max_workers * 4)
    for batch_start in range(0, len(all_pairs), batch_size):
        requests: list[dict[str, Any]] = []
        for company_id, security_id in all_pairs[batch_start : batch_start + batch_size]:
            sequence += 1
            form = {
                "cmpy_id": company_id,
                "security_id": security_id,
                "startDate": start_date,
                "endDate": end_date,
            }
            requests.append(
                {
                    "dataset": "price_history",
                    "object_store": object_store,
                    "url": BASE_URL + PRICE_HISTORY_SOURCE,
                    "method": "POST",
                    "post_form": form,
                    "request_params": form,
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "sequence": sequence,
                    "required": False,
                    "objects": objects,
                    "failures": failures,
                }
            )
        record_count += sum(_row_count(records) for records, _, _ in _fetch_many(fetcher, requests))
    return record_count, sequence


def _capture_documents(
    fetcher: PSEEdgeClient,
    *,
    object_store: ObjectStore,
    urls: set[str],
    ingested_at: datetime,
    run_id: str,
    sequence_start: int,
    objects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> tuple[int, int]:
    """Follow discovered PSE-hosted attachments in bounded concurrent batches."""
    sequence = _last_sequence(objects, sequence_start)
    document_limit = _env_limit("PSE_MAX_DOCUMENTS_PER_RUN")
    seen: set[str] = set()
    pending = set(urls)
    record_count = 0

    while pending and (document_limit is None or len(seen) < document_limit):
        remaining = fetcher.max_workers * 2
        if document_limit is not None:
            remaining = min(remaining, document_limit - len(seen))
        batch_limit = remaining
        batch = sorted(url for url in pending - seen if _document_url(url))[:batch_limit]
        if not batch:
            break
        seen.update(batch)
        requests = []
        for url in batch:
            sequence += 1
            requests.append(
                {
                    "dataset": "disclosure_assets",
                    "object_store": object_store,
                    "url": url,
                    "method": "GET",
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "sequence": sequence,
                    "required": False,
                    "objects": objects,
                    "failures": failures,
                }
            )
        for records, links, _ in _fetch_many(fetcher, requests):
            record_count += _row_count(records)
            pending.update(link for link in links if _document_url(link))
    if pending - seen:
        failures.append(
            {
                "dataset": "disclosure_assets",
                "url": "",
                "error": f"Document discovery stopped at configured PSE_MAX_DOCUMENTS_PER_RUN={document_limit}",
                "required": True,
                "sequence": sequence + 1,
                "truncated": True,
                "remaining_urls": len(pending - seen),
            }
        )
    return record_count, sequence


def _undocumented_sitemap_routes(links: list[str]) -> list[str]:
    """Return new, query-free PSE EDGE page routes listed in the site map."""
    configured_paths = {urlparse(path).path for path in PAGE_SOURCES.values()}
    discovered: set[str] = set()
    for link in links:
        parsed = urlparse(link)
        if (
            parsed.scheme in {"http", "https"}
            and (parsed.hostname or "").lower() == "edge.pse.com.ph"
            and parsed.path.lower().endswith(".do")
            and not parsed.query
            and parsed.path not in configured_paths
        ):
            discovered.add(link)
    return sorted(discovered)


def _scrape_with_client(
    scope: str,
    run_id: str,
    fetcher: PSEEdgeClient,
    *,
    object_store: ObjectStore,
    feed_urls: list[str] | None = None,
    full_history: bool = False,
) -> dict[str, str]:
    ingested_at = datetime.now(timezone.utc)
    raw_bucket = os.getenv("S3_BUCKET_RAW", "raw")
    objects: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    records_hint = 0
    sequence = 0
    search_window_days: int | None = None
    page_links: set[str] = set()
    company_ids: set[str] = set()
    security_pairs: set[tuple[str, str]] = set()
    document_urls: set[str] = set()
    sitemap_discovered_routes: list[str] = []

    job = (
        PSE_EDGE_CONFIGURED_SOURCES_JOB
        if scope == "configured_sources"
        else PSE_EDGE_JOBS.get(scope)
    )
    rss_feeds = (
        list(dict.fromkeys(feed_urls if feed_urls is not None else configured_rss_feeds()))
        if scope == "rss"
        else []
    )
    if scope == "rss" and not rss_feeds:
        failures.append({
            "dataset": "rss",
            "url": "",
            "error": "PSE_RSS_FEEDS contains no feed URLs",
            "required": scope == "rss",
            "sequence": 19_999,
        })

    if job is not None:
        search_window_days, date_window = _search_date_window(
            scope,
            job,
            full_history=full_history,
        )
        page_requests = []
        for sequence, dataset in enumerate(job.page_datasets, start=1):
            page_requests.append(
                {
                    "dataset": dataset,
                    "object_store": object_store,
                    "url": BASE_URL + PAGE_SOURCES[dataset],
                    "method": "GET",
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "sequence": sequence,
                    "required": True,
                    "objects": objects,
                    "failures": failures,
                }
            )
        sequence = len(page_requests)
        page_results = _fetch_many(fetcher, page_requests)
        sitemap_links: list[str] = []
        for dataset, (records, links, _) in zip(job.page_datasets, page_results):
            records_hint += _row_count(records)
            company_ids.update(_company_ids(records))
            security_pairs.update(_security_pairs(records))
            page_links.update(link for link in links if _document_url(link))
            if dataset == "site_map":
                sitemap_links = links

        sitemap_discovered_routes = _undocumented_sitemap_routes(sitemap_links)
        discovered_requests = []
        for route_no, route_url in enumerate(sitemap_discovered_routes, start=1):
            discovered_requests.append(
                {
                    "dataset": "sitemap_discovered_pages",
                    "object_store": object_store,
                    "url": route_url,
                    "method": "GET",
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "sequence": route_no,
                    "required": False,
                    "objects": objects,
                    "failures": failures,
                }
            )
        for records, links, _ in _fetch_many(fetcher, discovered_requests):
            records_hint += _row_count(records)
            company_ids.update(_company_ids(records))
            security_pairs.update(_security_pairs(records))
            page_links.update(link for link in links if _document_url(link))

        search_requests = [
            {
                "dataset": dataset,
                "object_store": object_store,
                "source": SEARCH_SOURCES[dataset],
                "ingested_at": ingested_at,
                "run_id": run_id,
                "objects": objects,
                "failures": failures,
                "sequence_start": 0,
                "date_window": date_window,
            }
            for dataset in job.search_datasets
        ]

        def fetch_search(request: dict[str, Any]) -> tuple[int, list[str], list[tuple[str, str]], list[str]]:
            return _fetch_search_pages(fetcher, **request)

        search_results = fetcher.map(fetch_search, search_requests)
        for total, ids, securities, links in search_results:
            records_hint += total
            company_ids.update(ids)
            security_pairs.update(securities)
            document_urls.update(links)

        company_ids.update(company_id for company_id, _ in security_pairs)
        if job.include_company_details:
            profile_count, sequence = _capture_company_details(
                fetcher,
                object_store=object_store,
                company_ids=company_ids,
                ingested_at=ingested_at,
                run_id=run_id,
                sequence_start=sequence,
                objects=objects,
                failures=failures,
                document_urls=document_urls,
                security_pairs=security_pairs,
            )
            records_hint += profile_count

        if job.include_price_history:
            history_count, sequence = _capture_price_history(
                fetcher,
                object_store=object_store,
                security_pairs=security_pairs,
                ingested_at=ingested_at,
                run_id=run_id,
                sequence_start=sequence,
                objects=objects,
                failures=failures,
            )
            records_hint += history_count

        if job.include_calendar_history:
            calendar_count, sequence = _capture_calendar_history(
                fetcher,
                object_store=object_store,
                ingested_at=ingested_at,
                run_id=run_id,
                sequence_start=sequence,
                objects=objects,
                failures=failures,
                document_urls=document_urls,
            )
            records_hint += calendar_count

        if job.follow_documents:
            document_count, sequence = _capture_documents(
                fetcher,
                object_store=object_store,
                urls=page_links | document_urls,
                ingested_at=ingested_at,
                run_id=run_id,
                sequence_start=sequence,
                objects=objects,
                failures=failures,
            )
            records_hint += document_count

    feed_requests = [
        {
            "feed_url": feed_url,
            "object_store": object_store,
            "feed_no": feed_no,
            "ingested_at": ingested_at,
            "run_id": run_id,
            "required": scope == "rss",
            "objects": objects,
        }
        for feed_no, feed_url in enumerate(rss_feeds, start=1)
    ]
    feed_results = fetcher.map(
        lambda request: _fetch_rss_feed(fetcher, **request),
        feed_requests,
    )
    for descriptor, failure in feed_results:
        if descriptor:
            records_hint += int(descriptor.get("records_found", 0))
        if failure:
            failures.append(failure)

    objects.sort(
        key=lambda item: (
            int(item.get("sequence", 0)),
            str(item.get("dataset", "")),
            str(item.get("url", "")),
        )
    )
    failures.sort(
        key=lambda item: (
            int(item.get("sequence", 0)),
            str(item.get("dataset", "")),
            str(item.get("url", "")),
        )
    )

    dataset_stats: dict[str, dict[str, int]] = {}
    for descriptor in objects:
        stats = dataset_stats.setdefault(
            descriptor["dataset"], {"responses": 0, "raw_bytes": 0, "records": 0}
        )
        stats["responses"] += 1
        stats["raw_bytes"] += int(descriptor.get("size_bytes", 0) or 0)
        stats["records"] += int(descriptor.get("records_found", 0) or 0)
    for dataset, stats in sorted(dataset_stats.items()):
        LOGGER.info(
            "PSE EDGE dataset captured: dataset=%s responses=%d raw_bytes=%d records=%d",
            dataset,
            stats["responses"],
            stats["raw_bytes"],
            stats["records"],
        )
    for failure in failures:
        log_failure = LOGGER.error if failure.get("required") else LOGGER.warning
        log_failure(
            "PSE EDGE source failure: dataset=%s required=%s url=%s error=%s",
            failure.get("dataset"),
            bool(failure.get("required")),
            failure.get("url"),
            failure.get("error"),
        )

    manifest = {
        "schema_version": 1,
        "source": "pse_edge",
        "scope": scope,
        "run_id": run_id,
        "started_at": ingested_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "base_url": BASE_URL,
        "search_window_days": search_window_days,
        "objects": objects,
        "failures": failures,
        "records_hint": records_hint,
    }
    if scope != "rss":
        manifest["portal_coverage"] = "public_catalog_attempted"
        manifest["route_inventory"] = {
            "catalog_page_routes": len(PAGE_SOURCES),
            "catalog_search_endpoints": len(SEARCH_SOURCES),
            "sitemap_discovered_page_routes": sitemap_discovered_routes,
            "company_detail_routes_per_issuer": len(COMPANY_DETAIL_SOURCES),
            "configured_scope_page_datasets": len(job.page_datasets) if job else 0,
            "configured_scope_search_datasets": len(job.search_datasets) if job else 0,
        }
        manifest["coverage_status"] = "incomplete_or_failed" if failures else "catalog_capture_succeeded"
        manifest["coverage_note"] = (
            "This records the configured public route and archive attempt. PSE EDGE may return empty, "
            "improved, or inaccessible pages, and an error-free capture does not prove the portal has "
            "no other data or hidden routes."
        )
    manifest_key = (
        f"pse_edge/manifests/ingest_date={ingested_at:%Y-%m-%d}/"
        f"run_id={_safe_segment(run_id)}.json"
    )
    object_store.write(
        raw_bucket,
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        content_type="application/json",
    )

    LOGGER.info(
        "PSE EDGE scrape complete: scope=%s raw_objects=%d records_hint=%d failures=%d manifest=%s",
        scope,
        len(objects),
        records_hint,
        len(failures),
        manifest_key,
    )
    return {"bucket": raw_bucket, "manifest_key": manifest_key, "run_id": run_id, "scope": scope}

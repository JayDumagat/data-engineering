"""Paced raw ingestion for the public PSE EDGE portal and configured RSS feeds."""

from __future__ import annotations

from datetime import datetime, timezone
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

from pipeline.sources.pse_edge.catalog import DOCUMENT_PATHS, PAGE_SOURCES, SEARCH_SOURCES
from pipeline.sources.pse_edge.client import PSEEdgeClient
from pipeline.sources.pse_edge.parser import parse_html_records, parse_json_records
from pipeline.storage.object_storage import upload_bytes


LOGGER = logging.getLogger(__name__)
BASE_URL = os.getenv("PSE_EDGE_BASE_URL", "https://edge.pse.com.ph").rstrip("/")
_APPEND_LOCK = threading.Lock()


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


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
        for link in record.get("links", []):
            onclick = link.get("onclick", "")
            match = re.search(r"\(\s*['\"]?(\d+)['\"]?\s*,", onclick)
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
    upload_bytes(bucket=bucket, key=key, data=payload, content_type=content_type)
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
    *,
    dataset: str,
    url: str,
    method: str,
    post_json: dict[str, Any] | None = None,
    ingested_at: datetime,
    run_id: str,
    sequence: int,
    required: bool,
    objects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], str]:
    try:
        response = fetcher.get(url) if method == "GET" else fetcher.post(url, json_body=post_json)
        descriptor = _store_raw(
            dataset=dataset,
            url=url,
            response=response,
            ingested_at=ingested_at,
            run_id=run_id,
            sequence=sequence,
        )
        if post_json is not None:
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
    *,
    dataset: str,
    path: str,
    ingested_at: datetime,
    run_id: str,
    objects: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    sequence_start: int,
) -> tuple[int, list[str], list[tuple[str, str]], list[str]]:
    max_pages = _env_int("PSE_MAX_SEARCH_PAGES", 100)
    row_total = 0
    company_ids: set[str] = set()
    security_pairs: set[tuple[str, str]] = set()
    document_urls: set[str] = set()
    for page_no in range(1, max_pages + 1):
        params: dict[str, Any]
        method = "POST"
        if dataset == "company_directory":
            params = {
                "pageNo": page_no,
                "companyId": "",
                "keyword": "",
                "sortType": "",
                "dateSortType": "DESC",
                "cmpySortType": "ASC",
                "symbolSortType": "ASC",
                "sector": "ALL",
                "subsector": "ALL",
            }
        elif dataset in {"dividends", "stock_rights"}:
            method = "GET"
            params = {
                "DividendsOrRights": "Dividends" if dataset == "dividends" else "Rights",
                "pageNo": page_no,
            }
        else:
            # The portal's search.ax actions are called with these query fields
            # by the public forms; blank filters ask the source for its full list.
            params = {
                "pageNo": page_no,
                "keyword": "",
                "tmplNm": "",
                "fromDate": "",
                "toDate": "",
            }
        url = _json_url(path, params)
        records, links, html = _fetch_and_store_page(
            fetcher,
            dataset=dataset,
            url=url,
            method=method,
            ingested_at=ingested_at,
            run_id=run_id,
            sequence=sequence_start + page_no,
            required=True,
            objects=objects,
            failures=failures,
        )
        row_count = _row_count(records)
        if row_count == 0:
            if page_no == 1 and not any(marker in html.lower() for marker in ("no data", "no records", "no result")):
                with _APPEND_LOCK:
                    failures.append(
                        {
                            "dataset": dataset,
                            "url": url,
                            "error": "PSE search response contained no data rows; check the endpoint or request fields",
                            "required": True,
                            "sequence": sequence_start + page_no,
                        }
                    )
            # Empty pages end pagination. The first page is still saved in raw.
            break
        row_total += row_count
        company_ids.update(_company_ids(records))
        security_pairs.update(_security_pairs(records))
        document_urls.update(link for link in links if _document_url(link))
        last_page = _page_count_hint(html)
        if last_page is not None and page_no >= last_page:
            break
    company_ids.update(company_id for company_id, _ in security_pairs)
    return row_total, sorted(company_ids), sorted(security_pairs), sorted(document_urls)


def _parse_feed_response(body: bytes, feed_url: str) -> list[dict[str, Any]]:
    from pipeline.sources.pse_edge.parser import parse_rss_records

    return parse_rss_records(body, feed_url)


def _fetch_rss_feed(
    fetcher: PSEEdgeClient,
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
        for link in record.get("links", []):
            numbers = re.findall(r"['\"]?(\d+)['\"]?", link.get("onclick", ""))
            if len(numbers) >= 2:
                pairs.add((numbers[0], numbers[1]))
    return sorted(pairs)


def _price_history_dates() -> tuple[str, str]:
    start_date = datetime.strptime(os.getenv("PSE_PRICE_HISTORY_START_DATE", "2000-01-01"), "%Y-%m-%d")
    end_date = datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Manila"))
    return start_date.strftime("%m-%d-%Y"), end_date.strftime("%m-%d-%Y")


def configured_rss_feeds() -> list[str]:
    """Return the ordered, deduplicated RSS/Atom feed URLs from configuration."""
    feeds = [
        item.strip()
        for item in os.getenv("PSE_RSS_FEEDS", "https://www.pse.com.ph/feed/").split(",")
        if item.strip()
    ]
    return list(dict.fromkeys(feeds))


def scrape(
    scope: str = "all",
    run_id: str | None = None,
    *,
    feed_urls: list[str] | None = None,
    max_concurrency: int | None = None,
) -> dict[str, str]:
    """Capture public PSE EDGE pages, search results, disclosure documents, and RSS.

    ``scope='rss'`` is the lightweight frequent lane. ``scope='all'`` captures
    the portal sitemap datasets and paginates the public search endpoints.
    Raw response bytes are persisted before any transformation.
    """
    if scope not in {"all", "rss"}:
        raise ValueError("scope must be either 'all' or 'rss'")
    source_run_id = run_id or uuid.uuid4().hex
    with PSEEdgeClient(worker_limit=max_concurrency) as fetcher:
        return _scrape_with_client(scope, source_run_id, fetcher, feed_urls=feed_urls)


def _scrape_with_client(
    scope: str,
    run_id: str,
    fetcher: PSEEdgeClient,
    *,
    feed_urls: list[str] | None = None,
) -> dict[str, str]:
    ingested_at = datetime.now(timezone.utc)
    raw_bucket = os.getenv("S3_BUCKET_RAW", "raw")
    objects: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    records_hint = 0
    sequence = 0
    page_links: set[str] = set()
    company_ids: set[str] = set()
    security_pairs: set[tuple[str, str]] = set()
    document_urls: set[str] = set()

    rss_feeds = list(dict.fromkeys(feed_urls if feed_urls is not None else configured_rss_feeds()))
    if not rss_feeds:
        failures.append({
            "dataset": "rss",
            "url": "",
            "error": "PSE_RSS_FEEDS contains no feed URLs",
            "required": scope == "rss",
            "sequence": 19_999,
        })

    if scope == "all":
        page_requests = []
        for sequence, (dataset, path) in enumerate(PAGE_SOURCES.items(), start=1):
            page_requests.append(
                {
                    "dataset": dataset,
                    "url": BASE_URL + path,
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
        for records, links, _ in page_results:
            records_hint += _row_count(records)
            page_links.update(link for link in links if _document_url(link))

        max_search_pages = _env_int("PSE_MAX_SEARCH_PAGES", 100)
        search_requests = [
            {
                "dataset": dataset,
                "path": path,
                "ingested_at": ingested_at,
                "run_id": run_id,
                "objects": objects,
                "failures": failures,
                "sequence_start": 1000 + offset * max_search_pages,
            }
            for offset, (dataset, path) in enumerate(SEARCH_SOURCES.items(), start=1)
        ]

        def fetch_search(request: dict[str, Any]) -> tuple[int, list[str], list[tuple[str, str]], list[str]]:
            return _fetch_search_pages(fetcher, **request)

        search_results = fetcher.map(fetch_search, search_requests)
        for total, ids, securities, links in search_results:
            records_hint += total
            company_ids.update(ids)
            security_pairs.update(securities)
            document_urls.update(links)

        # Company profile and highlights requests are independent once IDs are known.
        profile_limit = _env_int("PSE_MAX_COMPANY_PROFILES", 500)
        profile_requests: list[dict[str, Any]] = []
        for company_id in sorted(company_ids)[:profile_limit]:
            sequence += 1
            profile_requests.append(
                {
                    "dataset": "company_information",
                    "url": _json_url("/companyInformation/form.do", {"cmpy_id": company_id}),
                    "method": "GET",
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "sequence": sequence,
                    "required": False,
                    "objects": objects,
                    "failures": failures,
                }
            )
            sequence += 1
            profile_requests.append(
                {
                    "dataset": "company_financial_highlights",
                    "url": _json_url("/companyPage/financial_reports_view.do", {"cmpy_id": company_id}),
                    "method": "GET",
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "sequence": sequence,
                    "required": False,
                    "objects": objects,
                    "failures": failures,
                }
            )
        profile_results = _fetch_many(fetcher, profile_requests)
        for records, links, _ in profile_results:
            records_hint += _row_count(records)
            document_urls.update(link for link in links if _document_url(link))

        # Daily history calls are independent and the request pool caps concurrency.
        history_limit = _env_int("PSE_MAX_PRICE_HISTORIES_PER_RUN", 500)
        start_date, end_date = _price_history_dates()
        history_requests: list[dict[str, Any]] = []
        for company_id, security_id in sorted(security_pairs)[:history_limit]:
            sequence += 1
            query = {"cmpy_id": company_id, "security_id": security_id}
            params = {
                "cmpy_id": company_id,
                "security_id": security_id,
                "startDate": start_date,
                "endDate": end_date,
            }
            history_requests.append(
                {
                    "dataset": "price_history",
                    "url": _json_url("/common/DisclosureCht.ax", query),
                    "method": "POST",
                    "post_json": params,
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "sequence": sequence,
                    "required": False,
                    "objects": objects,
                    "failures": failures,
                }
            )
        history_results = _fetch_many(fetcher, history_requests)
        for records, _, _ in history_results:
            records_hint += _row_count(records)

        # Follow newly discovered document links in bounded concurrent batches.
        document_limit = _env_int("PSE_MAX_DOCUMENTS_PER_RUN", 100)
        seen_documents: set[str] = set()
        pending_documents = page_links | document_urls
        while pending_documents and len(seen_documents) < document_limit:
            batch_limit = min(fetcher.max_workers * 2, document_limit - len(seen_documents))
            batch = sorted(url for url in pending_documents - seen_documents if _document_url(url))[:batch_limit]
            if not batch:
                break
            seen_documents.update(batch)
            document_requests = []
            for url in batch:
                sequence += 1
                document_requests.append(
                    {
                        "dataset": "disclosure_assets",
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
            document_results = _fetch_many(fetcher, document_requests)
            for records, links, _ in document_results:
                records_hint += _row_count(records)
                pending_documents.update(link for link in links if _document_url(link))

    feed_requests = [
        {
            "feed_url": feed_url,
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
        "objects": objects,
        "failures": failures,
        "records_hint": records_hint,
    }
    manifest_key = (
        f"pse_edge/manifests/ingest_date={ingested_at:%Y-%m-%d}/"
        f"run_id={_safe_segment(run_id)}.json"
    )
    upload_bytes(
        bucket=raw_bucket,
        key=manifest_key,
        data=json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
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

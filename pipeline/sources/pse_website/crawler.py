"""Bounded, robots-aware, incremental crawler for public PSE website content."""

from __future__ import annotations

from datetime import datetime, timezone
from mimetypes import guess_extension, guess_type
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from zoneinfo import ZoneInfo
import hashlib
import json
import logging
import os
import re
import uuid

from pipeline.core.contracts import ObjectStore
from pipeline.sources.pse_edge.client import PSEHTTPClient, PSEHTTPError
from pipeline.sources.pse_edge.parser import parse_html_records, parse_rss_records
from pipeline.sources.pse_website.catalog import (
    ALLOWED_HOSTS,
    BASE_URL,
    DYNAMIC_DATASETS,
    PAGE_DATASETS,
    PAGE_HOSTS,
    PAGE_QUERY_KEYS,
    SEED_PATHS,
    host_allowed,
    is_asset_url,
)
from pipeline.storage.object_storage import get_object_store


LOGGER = logging.getLogger(__name__)
STATE_KEY = "pse_website/state/crawl_index_v2.json"
MANILA = ZoneInfo("Asia/Manila")
MAX_PAGE_LIMIT = 10_000


def _bounded_int(value: Any, default: int, maximum: int) -> int:
    try:
        return min(maximum, max(1, int(value)))
    except (TypeError, ValueError):
        return default


def _body(response: Any) -> bytes:
    value = getattr(response, "body", b"")
    return value if isinstance(value, bytes) else str(value).encode("utf-8", errors="replace")


def _headers(response: Any) -> dict[str, str]:
    return {str(key).lower(): str(value) for key, value in (getattr(response, "headers", {}) or {}).items()}


def _content_type(response: Any, url: str) -> str:
    headers = _headers(response)
    value = headers.get("content-type")
    if value:
        return value.split(";", 1)[0].strip().lower()
    guessed, _ = guess_type(urlparse(url).path)
    if guessed:
        return guessed
    return "text/html"


def _extension(content_type: str, url: str) -> str:
    if content_type == "application/pdf":
        return "pdf"
    if content_type in {"application/rss+xml", "application/atom+xml", "text/xml", "application/xml"}:
        return "xml"
    guessed = guess_extension(content_type) if content_type else None
    return (guessed or os.path.splitext(urlparse(url).path)[1] or ".bin").lstrip(".").lower()


def normalize_url(url: str, base: str | None = None) -> str | None:
    absolute = urljoin(base or BASE_URL, (url or "").strip())
    parsed = urlparse(absolute)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or host not in ALLOWED_HOSTS:
        return None
    scheme = "https"
    netloc = host
    path = parsed.path or "/"
    if host in PAGE_HOSTS and not is_asset_url(absolute) and path != "/":
        path = path.rstrip("/") + "/"
    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered.startswith("utm_") or lowered in {"fbclid", "gclid", "mc_cid", "mc_eid"}:
            continue
        # Do not turn arbitrary search/filter state into an unbounded crawl.
        if is_asset_url(absolute):
            query_pairs.append((key, value))
        elif lowered in PAGE_QUERY_KEYS:
            query_pairs.append((lowered, value))
    query_pairs.sort()
    return urlunparse((scheme, netloc, path, "", urlencode(query_pairs), ""))


def _likely_page(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.hostname in PAGE_HOSTS and not is_asset_url(url)


def _read_state(store: ObjectStore, bucket: str, key: str = STATE_KEY) -> dict[str, Any]:
    try:
        return json.loads(store.read(bucket, key))
    except Exception:
        return {"schema_version": 1, "urls": {}, "robots": {}}


def _robots_for_host(client: PSEHTTPClient, host: str) -> tuple[RobotFileParser, str, list[str]]:
    robots_url = f"https://{host}/robots.txt"
    parser = RobotFileParser(robots_url)
    try:
        response = client.get(robots_url)
        body = _body(response)
        text = body.decode("utf-8", errors="replace")
        parser.parse(text.splitlines())
        sitemaps = [
            line.split(":", 1)[1].strip()
            for line in text.splitlines()
            if line.strip().lower().startswith("sitemap:") and ":" in line
        ]
        return parser, text, sitemaps
    except PSEHTTPError as exc:
        if "HTTP 404" in str(exc):
            parser.parse([])
            return parser, "", []
        raise


def _allowed(parser: RobotFileParser, url: str, user_agent: str) -> bool:
    try:
        return parser.can_fetch(user_agent, url)
    except Exception:
        return False


def _inline_descriptor(
    *,
    bucket: str,
    key: str,
    url: str,
    dataset: str,
    content_type: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "url": url,
        "raw_bucket": bucket,
        "raw_key": key,
        "content_type": content_type,
        "inline_records": records,
    }


def configured_website_rss_feeds() -> list[str]:
    """Return validated PSE-hosted RSS/Atom feeds for the website source."""
    configured = os.getenv("PSE_WEBSITE_RSS_FEEDS", f"{BASE_URL}/feed/")
    feeds = list(dict.fromkeys(item.strip() for item in configured.split(",") if item.strip()))
    for feed_url in feeds:
        normalized = normalize_url(feed_url)
        if not normalized or (urlparse(normalized).hostname or "").lower() not in PAGE_HOSTS:
            raise ValueError(f"PSE website RSS feed must be on an official PSE page host: {feed_url}")
    return feeds


def _extract_market_report_rows(page: Any, request_delay_seconds: float) -> list[dict[str, Any]]:
    """Read the rendered PSE report table, including all paginated rows."""
    table_selector = "table[id^='ptp_']"
    page.wait_for_selector(f"{table_selector} tbody tr", state="visible", timeout=60_000)
    table = page.locator(table_selector).first
    table_id = table.get_attribute("id")
    if not table_id:
        raise RuntimeError("PSE market report table has no stable table id")

    header_locator = table.locator("thead tr").last.locator("th")
    headers = [value.strip() for value in header_locator.all_text_contents()]
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def collect_visible_rows() -> None:
        visible_rows = table.locator("tbody tr").evaluate_all(
            """elements => elements.map(row => ({
                cells: Array.from(row.querySelectorAll('td')).map(cell =>
                    (cell.innerText || cell.textContent || '').replace(/\\s+/g, ' ').trim()
                ),
                links: Array.from(row.querySelectorAll('a[href]')).map(link => ({
                    text: (link.innerText || link.textContent || '').replace(/\\s+/g, ' ').trim(),
                    href: link.href,
                    onclick: link.getAttribute('onclick') || ''
                }))
            }))"""
        )
        for row in visible_rows:
            values = row.get("cells", [])
            if not values or not any(str(value).strip() for value in values):
                continue
            columns = {
                (headers[index] if index < len(headers) and headers[index] else f"column_{index + 1}"): value
                for index, value in enumerate(values)
            }
            links = row.get("links", [])
            download_url = next((str(link.get("href", "")).strip() for link in links if link.get("href")), "")
            identity = download_url or json.dumps(columns, ensure_ascii=False, sort_keys=True)
            if identity in seen:
                continue
            seen.add(identity)
            normalized_columns = {str(key).strip().casefold(): value for key, value in columns.items()}
            report_title = next(
                (
                    str(normalized_columns[key]).strip()
                    for key in ("report type", "title", "report")
                    if normalized_columns.get(key) and str(normalized_columns[key]).strip()
                ),
                "PSE market report",
            )
            rows.append({
                "record_type": "table_row",
                "title": report_title,
                "columns": columns,
                "links": links,
                "download_url": download_url or None,
                "row_no": len(rows) + 1,
            })

    length_select = page.locator(f"#{table_id}_length")
    if length_select.count():
        try:
            length_select.select_option("100", timeout=5_000)
            page.wait_for_function(
                """({tableId, expected}) => {
                    const table = document.getElementById(tableId);
                    return table && table.querySelectorAll('tbody tr').length >= expected;
                }""",
                arg={"tableId": table_id, "expected": 50},
                timeout=30_000,
            )
        except Exception:
            # Some source versions expose only 10/25/50 rows per page.
            page.wait_for_timeout(max(500, int(request_delay_seconds * 1_000)))

    next_button = page.locator(f"#{table_id}_next")
    for _ in range(1_000):
        collect_visible_rows()
        if not next_button.count():
            break
        classes = (next_button.get_attribute("class") or "").casefold()
        if "disabled" in classes or (next_button.get_attribute("aria-disabled") or "").casefold() == "true":
            break
        first_row = table.locator("tbody tr").first.inner_text().strip()
        next_button.click(timeout=10_000)
        page.wait_for_function(
            """({tableId, previous}) => {
                const row = document.querySelector(`#${CSS.escape(tableId)} tbody tr`);
                return row && row.innerText.trim() !== previous;
            }""",
            arg={"tableId": table_id, "previous": first_row},
            timeout=30_000,
        )
        page.wait_for_timeout(max(500, int(request_delay_seconds * 1_000)))
    else:
        raise RuntimeError("PSE Market Reports pagination exceeded the 1,000-page safety cap")

    if not rows:
        raise RuntimeError("PSE Market Reports page rendered a table but returned no report rows")
    return rows


def _extract_dynamic_table_rows(
    page: Any,
    dataset: str,
    request_delay_seconds: float,
) -> list[dict[str, Any]]:
    """Capture every rendered row from archive/statistics tables and their pages."""
    page.wait_for_selector("table tbody tr", state="visible", timeout=60_000)
    tables = page.locator("table:visible")
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for table_no in range(tables.count()):
        table = tables.nth(table_no)
        table_id = table.get_attribute("id") or ""
        page_no = 0
        for _ in range(1_000):
            page_no += 1
            captured = table.evaluate(
                """table => {
                    const text = element => (element?.innerText || element?.textContent || '')
                        .replace(/\\s+/g, ' ').trim();
                    const heading = Array.from(document.querySelectorAll('h1,h2,h3,h4,h5,h6'))
                        .filter(item => item.compareDocumentPosition(table) & Node.DOCUMENT_POSITION_FOLLOWING)
                        .at(-1);
                    const caption = text(table.querySelector('caption'));
                    const headerRow = table.querySelector('thead tr:last-child')
                        || Array.from(table.querySelectorAll('tr')).find(row => row.querySelectorAll('th').length);
                    const headers = headerRow
                        ? Array.from(headerRow.querySelectorAll('th,td')).map(text)
                        : [];
                    const bodyRows = Array.from(table.querySelectorAll('tbody tr'));
                    return {
                        title: caption || text(heading),
                        headers,
                        rows: bodyRows.map(row => ({
                            cells: Array.from(row.querySelectorAll('th,td')).map(text),
                            links: Array.from(row.querySelectorAll('a[href],button[onclick]')).map(link => ({
                                text: text(link),
                                href: link.href || '',
                                onclick: link.getAttribute('onclick') || ''
                            })),
                            attributes: Object.fromEntries(Array.from(row.attributes)
                                .filter(attribute => attribute.name.startsWith('data-'))
                                .map(attribute => [attribute.name, attribute.value]))
                        }))
                    };
                }"""
            )
            headers = [str(value).strip() for value in captured.get("headers", [])]
            table_title = str(captured.get("title", "")).strip()
            visible_rows = captured.get("rows", [])
            if not visible_rows:
                break

            first_row_text = " ".join(str(value).strip() for value in visible_rows[0].get("cells", []))
            for row in visible_rows:
                values = [str(value).strip() for value in row.get("cells", [])]
                if not values or not any(values):
                    continue
                columns = {
                    (headers[index] if index < len(headers) and headers[index] else f"column_{index + 1}"): value
                    for index, value in enumerate(values)
                }
                links = row.get("links", [])
                identity = next(
                    (str(link.get("href", "")).strip() for link in links if link.get("href")),
                    json.dumps(
                        {"table_title": table_title, "columns": columns},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
                if identity in seen:
                    continue
                seen.add(identity)
                rows.append(
                    {
                        "record_type": "table_row",
                        "dataset": dataset,
                        "table_no": table_no + 1,
                        "table_title": table_title,
                        "row_no": len(rows) + 1,
                        "source_page": page_no,
                        "columns": columns,
                        "cells": values,
                        "links": links,
                        "source_attributes": row.get("attributes", {}),
                    }
                )

            next_clicked = page.evaluate(
                """({tableIndex, tableId}) => {
                    const visibleTables = Array.from(document.querySelectorAll('table'))
                        .filter(element => element.offsetWidth || element.offsetHeight || element.getClientRects().length);
                    const table = (tableId && document.getElementById(tableId)) || visibleTables[tableIndex];
                    const wrapper = table?.closest('.dataTables_wrapper')
                        || table?.closest('section, main')
                        || document;
                    const selectors = [
                        tableId ? `#${CSS.escape(tableId)}_next` : '',
                        '.dataTables_paginate .paginate_button.next',
                        '.pagination a[aria-label="Next"]',
                        '.pagination button[aria-label="Next"]',
                        'a[rel="next"]'
                    ].filter(Boolean);
                    let button = null;
                    for (const selector of selectors) {
                        button = wrapper.querySelector(selector);
                        if (button) break;
                    }
                    if (!button) return false;
                    const classes = (button.className || '').toString().toLowerCase();
                    if (button.disabled || button.getAttribute('aria-disabled') === 'true'
                        || classes.includes('disabled')) return false;
                    button.click();
                    return true;
                }""",
                {"tableIndex": table_no, "tableId": table_id},
            )
            if not next_clicked:
                break

            old_count = len(rows)
            try:
                page.wait_for_function(
                    """({tableIndex, tableId, previous}) => {
                        const visibleTables = Array.from(document.querySelectorAll('table'))
                            .filter(element => element.offsetWidth || element.offsetHeight || element.getClientRects().length);
                        const table = (tableId && document.getElementById(tableId)) || visibleTables[tableIndex];
                        const row = table?.querySelector('tbody tr');
                        return row && row.innerText.trim() !== previous;
                    }""",
                    arg={"tableIndex": table_no, "tableId": table_id, "previous": first_row_text},
                    timeout=30_000,
                )
            except Exception:
                # Avoid a duplicate loop when a site's next control is present but inert.
                if len(rows) == old_count:
                    break
            page.wait_for_timeout(max(500, int(request_delay_seconds * 1_000)))
        else:
            raise RuntimeError(f"PSE {dataset} pagination exceeded the 1,000-page safety cap")

    if not rows:
        raise RuntimeError(f"PSE {dataset} rendered tables but returned no non-empty rows")
    return rows


def crawl_public_site(
    *,
    run_id: str,
    scope: str = "public_site",
    options: dict[str, Any] | None = None,
    object_store: ObjectStore | None = None,
) -> dict[str, Any]:
    """Capture focused PSE data pages or RSS feeds to immutable raw storage."""
    options = options or {}
    if scope not in {"public_site", "rss"}:
        raise ValueError(f"Unsupported PSE Website crawl scope: {scope}")
    started_at = datetime.now(timezone.utc)
    ingest_local = started_at.astimezone(MANILA)
    run_segment = re.sub(r"[^A-Za-z0-9_.=-]+", "_", run_id).strip("_") or uuid.uuid4().hex
    max_pages = _bounded_int(options.get("max_pages", os.getenv("PSE_WEBSITE_MAX_PAGES", "10")), 10, MAX_PAGE_LIMIT)
    max_assets = 0
    worker_limit = options.get("max_concurrency")
    worker_limit = _bounded_int(worker_limit, 4, 16) if worker_limit is not None else None
    if scope == "public_site":
        worker_limit = min(worker_limit or 2, 2)
    bucket = os.getenv("S3_BUCKET_RAW", "raw")
    store = object_store if object_store is not None else get_object_store()
    state_key = STATE_KEY.replace(".json", f"_{scope}.json")
    previous = _read_state(store, bucket, state_key)
    old_urls = previous.get("urls", {}) if isinstance(previous, dict) else {}
    if not isinstance(old_urls, dict):
        old_urls = {}
    objects: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    robots: dict[str, RobotFileParser] = {}
    robots_text: dict[str, str] = {}
    robots_unavailable_hosts: set[str] = set()
    base_url = os.getenv("PSE_WEBSITE_BASE_URL", BASE_URL).rstrip("/")
    base_host = (urlparse(base_url).hostname or "").lower()
    if base_host not in PAGE_HOSTS:
        raise ValueError(f"PSE_WEBSITE_BASE_URL must use one of the official page hosts: {sorted(PAGE_HOSTS)}")
    configured_feeds = options.get("feed_urls")
    feed_urls = configured_feeds if configured_feeds is not None else configured_website_rss_feeds()
    if isinstance(feed_urls, str):
        feed_urls = [feed_urls]
    normalized_feeds = [normalize_url(str(item)) for item in feed_urls]
    if any(normalized is None for normalized in normalized_feeds):
        raise ValueError("Every PSE website RSS feed URL must use an official PSE page host")
    feed_urls = list(dict.fromkeys(normalized for normalized in normalized_feeds if normalized))
    if scope == "rss" and not feed_urls:
        raise ValueError("PSE_WEBSITE_RSS_FEEDS must contain at least one official PSE RSS feed")
    user_agent = os.getenv("PSE_WEBSITE_USER_AGENT", "Mozilla/5.0 (compatible; PSEPublicResearch/1.0)")

    LOGGER.info("PSE website capture started: run_id=%s scope=%s max_pages=%d", run_id, scope, max_pages)
    with PSEHTTPClient(
        worker_limit=worker_limit,
        base_url=base_url,
        user_agent=user_agent,
        concurrency_env="PSE_WEBSITE_CONCURRENCY",
        delay_env="PSE_WEBSITE_REQUEST_DELAY_SECONDS",
    ) as client:
        feed_hosts = {urlparse(url).hostname for url in feed_urls}
        robots_hosts = feed_hosts if scope == "rss" else {base_host}
        robots_disallowed_count = 0
        for host in sorted(host for host in robots_hosts if host):
            try:
                robots[host], robots_text[host], _ = _robots_for_host(client, host)
            except Exception as exc:
                robots_unavailable_hosts.add(host)
                failures.append({"dataset": "crawl_controls", "url": f"https://{host}/robots.txt", "error": str(exc), "required": True})
                LOGGER.warning("Could not read PSE robots.txt: host=%s error=%s", host, exc)

        robots_delay = max(
            (
                policy.crawl_delay(user_agent) or policy.crawl_delay("*") or 0.0
                for policy in robots.values()
            ),
            default=0.0,
        )
        client.delay = max(client.delay, robots_delay)
        LOGGER.info(
            "PSE website crawl pacing configured: request_delay_seconds=%.2f",
            client.delay,
        )
        if scope == "rss":
            seeds = [(url, "website_feeds") for url in feed_urls]
        else:
            seeds = [
                (url, PAGE_DATASETS[path])
                for path in SEED_PATHS
                if (url := normalize_url(f"{base_url}{path}"))
            ]
            saved_frontier = previous.get("frontier", []) if isinstance(previous, dict) else []
            if isinstance(saved_frontier, list):
                for item in saved_frontier:
                    if not isinstance(item, (list, tuple)) or len(item) != 2:
                        continue
                    saved_url = normalize_url(str(item[0]))
                    if saved_url:
                        seeds.append((saved_url, str(item[1])))
        frontier = []
        for url, dataset in seeds:
            host = (urlparse(url).hostname or "").lower()
            policy = robots.get(host)
            if not _likely_page(url) or policy is None:
                continue
            if not _allowed(policy, url, user_agent):
                robots_disallowed_count += 1
                continue
            frontier.append((url, dataset))
        visited: set[str] = set()
        next_state: dict[str, Any] = dict(old_urls)
        page_count = 0
        unchanged_count = 0
        sequence = 0
        asset_count = 0
        sitemap_seen: set[str] = set()

        while frontier and page_count < max_pages:
            batch: list[tuple[str, str]] = []
            deferred: list[tuple[str, str]] = []
            for url, dataset in frontier:
                normalized = normalize_url(url)
                if not normalized or normalized in visited or not _likely_page(normalized):
                    continue
                if page_count >= max_pages:
                    deferred.append((normalized, dataset))
                    continue
                page_count += 1
                visited.add(normalized)
                batch.append((normalized, dataset))
            if not batch:
                frontier = deferred
                break

            def fetch_one(item: tuple[str, str]) -> tuple[str, str, Any | None, Exception | None, list[dict[str, Any]]]:
                url, dataset = item
                prior = old_urls.get(url, {})
                validators: dict[str, str] = {}
                if prior.get("etag"):
                    validators["If-None-Match"] = prior["etag"]
                if prior.get("last_modified"):
                    validators["If-Modified-Since"] = prior["last_modified"]
                try:
                    if dataset in DYNAMIC_DATASETS:
                        captured_rows: list[dict[str, Any]] = []
                        is_market_report = dataset == "market_reports"

                        def page_action(page: Any) -> None:
                            if is_market_report:
                                captured_rows.extend(_extract_market_report_rows(page, client.delay))
                            else:
                                captured_rows.extend(
                                    _extract_dynamic_table_rows(page, dataset, client.delay)
                                )

                        response = client.get_dynamic(
                            url,
                            wait_selector="table[id^='ptp_'] tbody tr" if is_market_report else "table tbody tr",
                            page_action=page_action,
                        )
                        return url, dataset, response, None, captured_rows
                    return url, dataset, client.get(url, headers=validators or None), None, []
                except Exception as exc:
                    return url, dataset, None, exc, []

            results = client.map(fetch_one, batch)
            for url, dataset, response, error, captured_rows in results:
                if error is not None:
                    failures.append({
                        "dataset": dataset,
                        "url": url,
                        "error": str(error),
                        "required": scope == "rss" or dataset in PAGE_DATASETS.values(),
                    })
                    LOGGER.warning("PSE website fetch failed: url=%s error=%s", url, error)
                    continue
                status = int(getattr(response, "status", 200) or 200)
                if status == 304:
                    unchanged_count += 1
                    continue
                response_url = normalize_url(str(getattr(response, "url", url)))
                if response_url is None or not host_allowed(response_url):
                    failures.append({"dataset": dataset, "url": url, "error": "Redirect left the allowed PSE host list", "required": False})
                    continue
                payload = _body(response)
                content_type = _content_type(response, response_url)
                sequence += 1
                key = (
                    f"pse_website/raw/dataset={dataset}/ingest_date={ingest_local:%Y-%m-%d}/"
                    f"run_id={run_segment}/url={hashlib.sha256(url.encode()).hexdigest()}-"
                    f"content={hashlib.sha256(payload).hexdigest()}.{_extension(content_type, url)}"
                )
                store.write(bucket, key, payload, content_type)
                descriptor: dict[str, Any] = {
                    "dataset": dataset,
                    "sequence": sequence,
                    "url": url,
                    "resolved_url": response_url,
                    "raw_bucket": bucket,
                    "raw_key": key,
                    "content_type": content_type,
                    "status": status,
                    "size_bytes": len(payload),
                }
                inline_records: list[dict[str, Any]] = []
                if captured_rows:
                    inline_records.extend(captured_rows)
                    descriptor["records_found"] = len(captured_rows)
                elif dataset == "website_feeds" or "rss" in content_type or "atom" in content_type:
                    try:
                        inline_records.extend(parse_rss_records(payload, url))
                        descriptor["records_found"] = len(inline_records)
                    except Exception as exc:
                        failures.append({"dataset": dataset, "url": url, "error": f"Could not parse RSS/Atom feed: {exc}", "required": scope == "rss"})
                elif "html" in content_type or payload.lstrip().startswith((b"<", b"<!DOCTYPE")):
                    try:
                        parsed_records, _ = parse_html_records(payload.decode("utf-8", errors="replace"), dataset, url)
                        descriptor["records_found"] = sum(record.get("record_type") == "table_row" for record in parsed_records)
                    except Exception as exc:
                        failures.append({"dataset": dataset, "url": url, "error": f"Could not parse source page: {exc}", "required": dataset in PAGE_DATASETS.values()})
                if dataset in DYNAMIC_DATASETS and not int(descriptor.get("records_found", 0) or 0):
                    failures.append(
                        {
                            "dataset": dataset,
                            "url": url,
                            "error": "Rendered source returned no non-empty table rows",
                            "required": True,
                        }
                    )
                if inline_records:
                    descriptor["inline_records"] = inline_records
                objects.append(descriptor)
                response_headers = _headers(response)
                next_state[url] = {
                    "raw_bucket": bucket,
                    "raw_key": key,
                    "content_type": content_type,
                    "etag": response_headers.get("etag"),
                    "last_modified": response_headers.get("last-modified"),
                    "fetched_at": started_at.isoformat(),
                }

            frontier = deferred

        truncated = bool(frontier)
        page_datasets = set(PAGE_DATASETS.values())
        if robots_unavailable_hosts:
            coverage_status = "robots_policy_unavailable"
        elif any(failure.get("dataset") in page_datasets or failure.get("dataset") == "website_feeds" for failure in failures):
            coverage_status = "incomplete_source_failures"
        elif robots_disallowed_count and page_count == 0 and asset_count == 0:
            coverage_status = "blocked_by_robots"
            failures.append(
                {
                    "dataset": "crawl_controls",
                    "url": f"https://{base_host}/robots.txt",
                    "error": "Robots policy disallows every discovered URL for the configured crawler",
                    "required": True,
                }
            )
        elif robots_disallowed_count:
            coverage_status = "robots_restricted"
        elif truncated:
            coverage_status = "incomplete_truncated"
        else:
            coverage_status = "focused_sources_captured"
        # Robots directives and a crawl summary are always retained as structured controls.
        control_records = [
            {"record_type": "robots_policy", "host": host, "robots_url": f"https://{host}/robots.txt", "text": text[:100_000]}
            for host, text in sorted(robots_text.items())
        ]
        control_records.append(
            {
                "record_type": "crawl_summary",
                "run_id": run_id,
                "visited_pages": page_count,
                "visited_assets": asset_count,
                "unchanged_urls": unchanged_count,
                "truncated": truncated,
                "pending_urls": len(frontier),
                "robots_disallowed_urls": robots_disallowed_count,
                "coverage_status": coverage_status,
                "sitemaps_seen": len(sitemap_seen),
            }
        )
        controls_payload = json.dumps(control_records, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        controls_key = (
            f"pse_website/raw/dataset=website_crawl_controls/ingest_date={ingest_local:%Y-%m-%d}/"
            f"run_id={run_segment}/controls-{hashlib.sha256(controls_payload).hexdigest()}.json"
        )
        store.write(bucket, controls_key, controls_payload, "application/json")
        objects.append(_inline_descriptor(
            bucket=bucket,
            key=controls_key,
            url=f"{base_url}/robots.txt",
            dataset="website_crawl_controls",
            content_type="application/json",
            records=control_records,
        ))

    completed_at = datetime.now(timezone.utc)
    manifest = {
        "schema_version": 1,
        "source": "pse_website",
        "scope": scope,
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "objects": objects,
        "failures": failures,
        "visited_pages": page_count,
        "visited_assets": asset_count,
        "unchanged_urls": unchanged_count,
        "sitemaps_seen": len(sitemap_seen),
        "truncated": truncated,
        "pending_urls": len(frontier),
        "robots_disallowed_urls": robots_disallowed_count,
        "coverage_status": coverage_status,
        "limits": {"max_pages": max_pages, "max_assets": max_assets},
    }
    manifest_payload = json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    manifest_key = (
        f"pse_website/manifests/ingest_date={ingest_local:%Y-%m-%d}/"
        f"run_id={run_segment}-{hashlib.sha256(manifest_payload).hexdigest()[:16]}.json"
    )
    store.write(bucket, manifest_key, manifest_payload, "application/json")
    # Persist validators only after the run manifest is durable.
    state = {
        "schema_version": 1,
        "updated_at": completed_at.isoformat(),
        "urls": next_state,
        "robots": robots_text,
        "frontier": sorted(set(frontier)),
    }
    store.write(bucket, state_key, json.dumps(state, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), "application/json")
    LOGGER.info(
        "PSE website capture complete: run_id=%s pages=%d assets=%d unchanged=%d failures=%d "
        "pending=%d robots_disallowed=%d coverage=%s truncated=%s manifest=s3://%s/%s",
        run_id,
        page_count,
        asset_count,
        unchanged_count,
        len(failures),
        len(frontier),
        robots_disallowed_count,
        coverage_status,
        truncated,
        bucket,
        manifest_key,
    )
    return {
        "run_id": run_id,
        "scope": scope,
        "bucket": bucket,
        "manifest_key": manifest_key,
        **{
            key: manifest[key]
            for key in (
                "visited_pages",
                "visited_assets",
                "unchanged_urls",
                "pending_urls",
                "robots_disallowed_urls",
                "coverage_status",
                "truncated",
            )
        },
    }

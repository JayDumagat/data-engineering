"""Bounded, robots-aware, incremental crawler for public PSE website content."""

from __future__ import annotations

from datetime import datetime, timezone
from html.parser import HTMLParser
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
    DOCUMENT_HOSTS,
    PAGE_HOSTS,
    PAGE_QUERY_KEYS,
    SEED_PATHS,
    SITEMAP_PATHS,
    host_allowed,
    is_asset_url,
)
from pipeline.storage.object_storage import get_object_store


LOGGER = logging.getLogger(__name__)
STATE_KEY = "pse_website/state/crawl_index.json"
MANILA = ZoneInfo("Asia/Manila")
MAX_PAGE_LIMIT = 10_000
MAX_ASSET_LIMIT = 10_000


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
    if parsed.hostname in PAGE_HOSTS:
        if is_asset_url(url):
            return False
        if parsed.path.lower().endswith((".php", ".json", ".js", ".css", ".txt")):
            return False
        if any(token in parsed.path.lower() for token in ("/wp-admin", "/wp-login", "/wp-json/", "/api/")):
            return False
        return True
    return parsed.hostname in DOCUMENT_HOSTS and is_asset_url(url)


class _LocParser(HTMLParser):
    """Extract sitemap loc/lastmod pairs without loading a sitemap library."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.entries: list[dict[str, str]] = []
        self._entry: dict[str, str] | None = None
        self._field: str | None = None
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        name = tag.rsplit(":", 1)[-1].lower()
        if name in {"url", "sitemap"}:
            self._entry = {}
        elif name in {"loc", "lastmod"} and self._entry is not None:
            self._field = name
            self._parts = []

    def handle_data(self, data: str) -> None:
        if self._field:
            self._parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        name = tag.rsplit(":", 1)[-1].lower()
        if name == self._field and self._entry is not None:
            self._entry[name] = "".join(self._parts).strip()
            self._field = None
        elif name in {"url", "sitemap"} and self._entry:
            if self._entry.get("loc"):
                self.entries.append(self._entry)
            self._entry = None


def _read_state(store: ObjectStore, bucket: str) -> dict[str, Any]:
    try:
        return json.loads(store.read(bucket, STATE_KEY))
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


def crawl_public_site(
    *,
    run_id: str,
    options: dict[str, Any] | None = None,
    object_store: ObjectStore | None = None,
) -> dict[str, Any]:
    """Capture public HTML, sitemaps, feeds and linked files to immutable raw storage."""
    options = options or {}
    started_at = datetime.now(timezone.utc)
    ingest_local = started_at.astimezone(MANILA)
    run_segment = re.sub(r"[^A-Za-z0-9_.=-]+", "_", run_id).strip("_") or uuid.uuid4().hex
    max_pages = _bounded_int(options.get("max_pages", os.getenv("PSE_WEBSITE_MAX_PAGES", "3000")), 3000, MAX_PAGE_LIMIT)
    max_assets = _bounded_int(options.get("max_assets", os.getenv("PSE_WEBSITE_MAX_ASSETS", "3000")), 3000, MAX_ASSET_LIMIT)
    worker_limit = options.get("max_concurrency")
    worker_limit = _bounded_int(worker_limit, 4, 16) if worker_limit is not None else None
    bucket = os.getenv("S3_BUCKET_RAW", "raw")
    store = object_store if object_store is not None else get_object_store()
    previous = _read_state(store, bucket)
    old_urls = previous.get("urls", {}) if isinstance(previous, dict) else {}
    if not isinstance(old_urls, dict):
        old_urls = {}
    pending_urls = previous.get("frontier", []) if isinstance(previous, dict) else []
    if not isinstance(pending_urls, list):
        pending_urls = []
    objects: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    robots: dict[str, RobotFileParser] = {}
    robots_text: dict[str, str] = {}
    sitemap_urls: list[str] = []
    base_url = os.getenv("PSE_WEBSITE_BASE_URL", BASE_URL).rstrip("/")
    base_host = (urlparse(base_url).hostname or "").lower()
    if base_host not in PAGE_HOSTS:
        raise ValueError(f"PSE_WEBSITE_BASE_URL must use one of the official page hosts: {sorted(PAGE_HOSTS)}")
    user_agent = os.getenv("PSE_WEBSITE_USER_AGENT", "Mozilla/5.0 (compatible; PSEPublicResearch/1.0)")

    LOGGER.info("PSE public website crawl started: run_id=%s max_pages=%d max_assets=%d", run_id, max_pages, max_assets)
    with PSEHTTPClient(
        worker_limit=worker_limit,
        base_url=base_url,
        user_agent=user_agent,
        concurrency_env="PSE_WEBSITE_CONCURRENCY",
        delay_env="PSE_WEBSITE_REQUEST_DELAY_SECONDS",
    ) as client:
        for host in sorted(ALLOWED_HOSTS):
            try:
                robots[host], robots_text[host], host_sitemaps = _robots_for_host(client, host)
                sitemap_urls.extend(host_sitemaps)
            except Exception as exc:
                failures.append({"dataset": "crawl_controls", "url": f"https://{host}/robots.txt", "error": str(exc), "required": host == base_host})
                LOGGER.warning("Could not read PSE robots.txt: host=%s error=%s", host, exc)

        page_robots = robots.get(base_host)
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
        seed_urls = [*pending_urls, *(f"{base_url}{path}" for path in SEED_PATHS)]
        seed_urls.extend(sitemap_urls)
        frontier = [url for raw_url in seed_urls if page_robots is not None and (url := normalize_url(raw_url)) and _likely_page(url)]
        # Add a sitemap URL even if robots.txt did not advertise one.
        frontier.extend(
            url for path in SITEMAP_PATHS
            if page_robots is not None
            and (url := normalize_url(f"{base_url}{path}")) and url not in frontier
        )
        visited: set[str] = set()
        next_state: dict[str, Any] = dict(old_urls)
        sitemap_seen: set[str] = set()
        page_count = 0
        asset_count = 0
        unchanged_count = 0
        robots_disallowed_count = 0
        sequence = 0

        while frontier and (page_count < max_pages or asset_count < max_assets):
            batch: list[tuple[str, str]] = []
            deferred: set[str] = set()
            for url in frontier:
                normalized = normalize_url(url)
                if not normalized or normalized in visited or not _likely_page(normalized):
                    continue
                host = (urlparse(normalized).hostname or "").lower()
                policy = robots.get(host)
                if policy is None or not _allowed(policy, normalized, user_agent):
                    visited.add(normalized)
                    robots_disallowed_count += 1
                    continue
                if is_asset_url(normalized):
                    if asset_count >= max_assets:
                        deferred.add(normalized)
                        continue
                    asset_count += 1
                    dataset = "website_assets"
                else:
                    if page_count >= max_pages:
                        deferred.add(normalized)
                        continue
                    page_count += 1
                    dataset = "website_sitemaps" if normalized.lower().endswith(".xml") or "sitemap" in normalized.lower() else (
                        "website_feeds" if "/feed" in normalized.lower() else "website_pages"
                    )
                visited.add(normalized)
                batch.append((normalized, dataset))
            if not batch:
                frontier = sorted(deferred)
                break

            def fetch_one(item: tuple[str, str]) -> tuple[str, str, Any | None, Exception | None]:
                url, dataset = item
                prior = old_urls.get(url, {})
                validators: dict[str, str] = {}
                if prior.get("etag"):
                    validators["If-None-Match"] = prior["etag"]
                if prior.get("last_modified"):
                    validators["If-Modified-Since"] = prior["last_modified"]
                try:
                    return url, dataset, client.get(url, headers=validators or None), None
                except Exception as exc:
                    return url, dataset, None, exc

            results = client.map(fetch_one, batch)
            discovered: list[str] = []
            for url, dataset, response, error in results:
                if error is not None:
                    failures.append({"dataset": dataset, "url": url, "error": str(error), "required": url == normalize_url(base_url)})
                    LOGGER.warning("PSE website fetch failed: url=%s error=%s", url, error)
                    continue
                status = int(getattr(response, "status", 200) or 200)
                if status == 304:
                    unchanged_count += 1
                    prior = old_urls.get(url, {})
                    discovered.extend(prior.get("outlinks", []))
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
                outlinks: list[str] = []
                inline_records: list[dict[str, Any]] = []
                if dataset == "website_sitemaps" or (dataset != "website_feeds" and
                    "xml" in content_type and "rss" not in content_type and "atom" not in content_type
                ):
                    parser = _LocParser()
                    parser.feed(payload.decode("utf-8", errors="replace"))
                    for row in parser.entries:
                        candidate = normalize_url(row.get("loc", ""), url)
                        if candidate and _likely_page(candidate):
                            outlinks.append(candidate)
                            inline_records.append({"record_type": "sitemap_url", "loc": candidate, "lastmod": row.get("lastmod", "")})
                    sitemap_seen.add(url)
                elif "html" in content_type or payload.lstrip().startswith((b"<", b"<!DOCTYPE")):
                    try:
                        parsed_records, links = parse_html_records(payload.decode("utf-8", errors="replace"), dataset, url)
                        outlinks.extend(candidate for raw_link in links if (candidate := normalize_url(raw_link, url)) and _likely_page(candidate))
                        descriptor["records_found"] = len(parsed_records)
                    except Exception as exc:
                        LOGGER.warning("Could not parse PSE website HTML links: url=%s error=%s", url, exc)
                elif dataset == "website_feeds" or "rss" in content_type or "atom" in content_type:
                    try:
                        feed_records = parse_rss_records(payload, url)
                        inline_records.extend(feed_records)
                        outlinks.extend(candidate for item in feed_records if (candidate := normalize_url(str(item.get("link", "")), url)))
                    except Exception as exc:
                        failures.append({"dataset": dataset, "url": url, "error": f"Could not parse feed: {exc}", "required": False})
                # Keep crawl edges explicit in the curated record set and in state.
                descriptor["outlinks"] = sorted(set(outlinks))
                if inline_records:
                    descriptor["inline_records"] = inline_records
                objects.append(descriptor)
                discovered.extend(descriptor["outlinks"])
                response_headers = _headers(response)
                next_state[url] = {
                    "raw_bucket": bucket,
                    "raw_key": key,
                    "content_type": content_type,
                    "etag": response_headers.get("etag"),
                    "last_modified": response_headers.get("last-modified"),
                    "outlinks": descriptor["outlinks"],
                    "fetched_at": started_at.isoformat(),
                }

            frontier = sorted((set(discovered) | deferred) - visited)

        truncated = bool(frontier)
        if robots_disallowed_count and page_count == 0 and asset_count == 0:
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
            coverage_status = "discovered_urls_captured"
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
        "scope": "public_site",
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
    # Persist validators and link graph only after the run manifest is durable.
    state = {
        "schema_version": 1,
        "updated_at": completed_at.isoformat(),
        "urls": next_state,
        "robots": robots_text,
        "frontier": sorted(set(frontier)),
    }
    store.write(bucket, STATE_KEY, json.dumps(state, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), "application/json")
    LOGGER.info(
        "PSE public website crawl complete: run_id=%s pages=%d assets=%d unchanged=%d failures=%d "
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

"""Public PSE pages and strict host/path boundaries for the website crawl."""

from __future__ import annotations

from urllib.parse import urlparse


BASE_URL = "https://www.pse.com.ph"
PAGE_HOSTS = frozenset({"www.pse.com.ph", "pse.com.ph"})
DOCUMENT_HOSTS = frozenset({"documents.pse.com.ph"})
ALLOWED_HOSTS = PAGE_HOSTS | DOCUMENT_HOSTS

# Collect records from PSE's data pages and publication archives. Broad site
# navigation and sitemaps contain menus, legal pages, and unrelated material.
PAGE_DATASETS = {
    "/market-report/": "market_reports",
    "/news-and-announcement-archive/": "announcements",
    "/press-room-archive/": "press_releases",
    "/listing-statistics/": "listing_statistics",
}
SEED_PATHS = tuple(PAGE_DATASETS)
DYNAMIC_DATASETS = frozenset(PAGE_DATASETS.values())
ASSET_SUFFIXES = frozenset(
    {
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip",
        ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp",
        ".mp4", ".mov", ".webm", ".ppt", ".pptx", ".odt", ".ods",
    }
)
PAGE_QUERY_KEYS = frozenset({"page", "paged", "sf_paged", "page_no", "pageno"})


def host_allowed(url: str) -> bool:
    return (urlparse(url).hostname or "").lower() in ALLOWED_HOSTS


def is_asset_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.path.lower().endswith(tuple(ASSET_SUFFIXES)) or (
        (parsed.hostname or "").lower() in DOCUMENT_HOSTS
    )

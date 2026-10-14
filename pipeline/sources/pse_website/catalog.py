"""Public PSE pages and strict host/path boundaries for the website crawl."""

from __future__ import annotations

from urllib.parse import urlparse


BASE_URL = "https://www.pse.com.ph"
PAGE_HOSTS = frozenset({"www.pse.com.ph", "pse.com.ph"})
DOCUMENT_HOSTS = frozenset({"documents.pse.com.ph"})
ALLOWED_HOSTS = PAGE_HOSTS | DOCUMENT_HOSTS

# Focused entry points cover the main public information areas. Sitemaps add
# pages the public navigation does not expose directly.
SEED_PATHS = (
    "/",
    "/news_announcement/",
    "/press-room-archive/",
    "/news-and-announcement-archive/",
    "/market-report/",
    "/data-products/",
    "/listed-company-directory/",
    "/listing-statistics/",
    "/investing-at-pse/",
    "/feed/",
    "/wp-sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap.xml",
)

SITEMAP_PATHS = ("/wp-sitemap.xml", "/sitemap_index.xml", "/sitemap.xml")
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

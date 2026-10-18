# PSE public website source

`PSEWebsiteConnector` captures the public `www.pse.com.ph` site and its canonical apex hostname. It follows same-site links and sitemaps, captures the public RSS feed, and saves publicly linked files from `documents.pse.com.ph` or the main PSE host. The crawler does not access PSE EDGE, other PSE subdomains, external company sites, authenticated pages, or paid/licensed products.

Each request is checked against the host's `robots.txt`; a policy that blocks all discovered URLs is recorded as a required source failure instead of a successful empty crawl. Crawl concurrency and request pacing are bounded by Airflow's `pse_website_source` pool and `PSE_WEBSITE_*` environment settings. ETag and Last-Modified validators, the discovered link graph, and any deferred frontier live in `raw/pse_website/state/crawl_index.json`; later runs resume deferred URLs. Changed responses are stored as immutable objects under `raw/pse_website/raw/`. Every run writes a manifest with counts, coverage status, truncation state, and failures under `raw/pse_website/manifests/`.

HTML pages, feed entries, sitemap rows, and crawl controls are normalized to Parquet. Public document and image bytes remain in object storage; their metadata is kept in the lake and is excluded from PostgreSQL. Page/feed/sitemap records and ingestion audits are loaded to the separate `pse_website` PostgreSQL schema. The Airflow DAG runs daily and publishes a warehouse asset consumed by the dbt analytics DAG.

Defaults are 3,000 pages, 3,000 linked assets, four HTTP workers, and a 0.75-second request start interval. Set `PSE_WEBSITE_MAX_PAGES` or `PSE_WEBSITE_MAX_ASSETS` to raise the crawl caps. The manifest marks a crawl as truncated when more eligible URLs remain. Some market-data products are licensed, request-based, or gated; this connector only captures material exposed as public pages or public linked files. Follow the site footer terms for collected material.

For a manual bounded capture, run `python -m tools.pse_website_fetch --max-pages 250 --max-assets 100` with the object-store environment configured. The scheduled Airflow DAG is `pse_website_public_ingestion`.

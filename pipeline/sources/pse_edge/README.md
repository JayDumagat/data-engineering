# PSE EDGE source

The source connector captures PSE EDGE's public site-map routes, form-backed search endpoints, issuer pages, historical market views, and PSE-hosted files. Each domain remains an independently runnable job; a separate full-catalog scope is available for the initial backfill. The live [PSE EDGE site map](https://edge.pse.com.ph/page/siteMap.do) and its linked public pages define the route inventory.

## Coverage

| Data area | Capture strategy |
| --- | --- |
| Portal pages and footer | Home, site map, About, FAQ, disclaimer, privacy, link policy, and contact pages |
| Company directory | Form-encoded search across every available page; company and security IDs are extracted from directory rows |
| Company pages | Stock data, company information, directors and management, financial reports, disclosures, and dividend/right result lists for every discovered issuer |
| Exchange reports and notices | Company announcements, financial and other reports, disclosure notices, listing notices, and company disclosures, all paginated |
| Advanced and special searches | Advanced Search with `*` and unbounded date range, PSEi disclosures, ETF disclosures, dividends, and stock rights |
| Market data | Index summary snapshot, halts and suspensions, listing applicants, watch list, and ETF/PSEi search pages |
| Market history | Stock history for each discovered company/security pair from `PSE_PRICE_HISTORY_START_DATE`; monthly calendar pages from 2016 through the current Manila month |
| Files | PSE-hosted documents and attachments discovered in captured pages and search results, followed recursively |
| New site-map pages | Any additional query-free `.do` routes found on the site map are captured as `sitemap_discovered_pages` |

The full-catalog scope is exhaustive over the configured public catalog and the routes discovered from the public site map. It does not claim that PSE EDGE exposes every historical record through those routes. Some public pages currently return no rows or say they are being improved; those responses are retained as raw captures. A source error, repeated page, or configured positive limit is recorded in the run manifest so a partial run cannot look complete.

PSE EDGE's footer states that its materials may be accessed and downloaded only for personal, non-commercial use unless otherwise stated. See the current [PSE disclaimer and access restrictions](https://edge.pse.com.ph/page/disclaimer.do) before using captured material outside that scope.

## Run lanes

- `pse_edge_full_ingestion` runs the named current-state jobs on weekdays at 03:15 Asia/Manila. Airflow maps one capture-transform-load group per domain, with independent retries, run IDs, manifests, storage objects, and warehouse audits. The default scheduled set excludes issuer profiles, historical stock/calendar views, and the unfiltered company-disclosure archive.
- Trigger `pse_edge_full_ingestion` with `{"jobs": ["all"]}` to map the full backfill into independent domain capture-transform-load paths, with date filters open. The set avoids duplicate directory/profile pulls by sourcing those records through `company_market_history`. `{"jobs": ["configured_sources"]}` remains available as a single-scope full capture. For focused backfills, run a named job with `{"jobs": ["financial_reports"], "full_history": true}` or run `company_disclosures`, `company_profiles`, `company_market_history`, or `market_calendar_archive`.
- RSS from `www.pse.com.ph` is captured by `pse_website_rss_ingestion` and loaded into the `pse_website` schema. The optional PSE EDGE RSS scope accepts only feeds on the configured EDGE host.
- `pse_edge_analytics` runs dbt after successful PSE EDGE, RSS, or PSE website warehouse loads.

The capture client uses pooled Scrapling sessions, form-encoded POSTs for PSE EDGE's AJAX searches, bounded concurrent batches, and one shared request pace across workers. Scheduled search jobs use a 14-day overlap where the source supports date filters; the full-catalog scope leaves dates blank to backfill all available history. It does not work around anti-bot challenges. A challenge or access-denied response is recorded as a failed source request.

## Source and ETL boundaries

`PSEEdgeConnector` implements the shared `SourceConnector` contract and writes immutable raw responses through `ObjectStore`. The page and search catalog lives in `catalog.py`; request transport, pagination, extraction, and storage live in `client.py`, `parser.py`, and `scraper.py`.

The ETL runner reads each source manifest and sends raw objects to a source-specific `Transformer`. Transformation and validation are separated from loading. Normalized records use Polars and Pandera; `DatasetRouter` selects destinations, and `DataSink` adapters implement them independently. S3-compatible raw/lake storage, PostgreSQL, and quarantine sinks can be replaced without changing PSE request logic.

### Add a source

Add the public route or `SearchSource` request shape to `catalog.py`, then assign it to a `PSEEdgeJob`. The mapped Airflow capture-transform-load group will schedule the named job. Add a source-specific parser or transformation only when its record shape requires one, and update `DatasetRouter` only when its destination differs.

## Storage

| Data | Destination | Notes |
| --- | --- | --- |
| Original HTML, JSON, RSS/XML, PDFs, images, videos, and other attachments | `raw` object-store bucket | Immutable source response bytes |
| Normalized records | `stage` object-store bucket | Silver Zstandard Parquet run deltas, deduplicated by `(dataset, record_key)` per run |
| Curated current records | `curated` object-store bucket | One globally deduplicated `current.parquet` per source and dataset under `curated/current/` |
| Searchable records and ingestion audit | PostgreSQL `pse_edge` schema | Unique `(dataset, record_key)`, full JSONB payload, typed search fields, object pointers, first/last-seen times |
| Transform errors | `quarantine` object-store bucket | JSON diagnostics retained for replay |
| Airflow task logs | `airflow-logs` object-store bucket | Remote S3 logs plus local task copies |
| Analytics models | PostgreSQL `analytics` schema | dbt staging views and materialized fact tables |

`DatasetRouter` sends attachment metadata and binary-asset metadata to object storage only. Original bytes stay in the raw bucket. Warehouse loads use each run's stage delta, while curated snapshots merge new records with prior records by stable key. Existing per-run curated Parquet objects are folded into the first current snapshot automatically.

## Configuration

All `PSE_MAX_*` variables default to `0`, which means no application-level cap. A positive value is an explicit cap; reaching it creates a required `truncated` failure in the manifest. Leave them at zero for the full public-catalog run.

| Variable | Default | Purpose |
| --- | --- | --- |
| `PSE_EDGE_BASE_URL` | `https://edge.pse.com.ph` | Public PSE EDGE base URL |
| `PSE_MAX_SEARCH_PAGES` | `0` | Page cap per search endpoint |
| `PSE_MAX_COMPANY_PROFILES` | `0` | Issuer profile cap per run |
| `PSE_MAX_PRICE_HISTORIES_PER_RUN` | `0` | Company/security history cap per run |
| `PSE_PRICE_HISTORY_START_DATE` | `2000-01-01` | First date requested for stock histories |
| `PSE_CALENDAR_START_YEAR` | `2016` | First year in the public calendar selector |
| `PSE_MAX_CALENDAR_MONTHS` | `0` | Calendar-month cap per run |
| `PSE_MAX_DOCUMENTS_PER_RUN` | `0` | Discovered document cap per run |
| `PSE_EDGE_CONCURRENCY` | `6` | HTTP workers, bounded to 1–16 and Airflow pool capacity |
| `PSE_REQUEST_DELAY_SECONDS` | `0.5` | Shared request interval; Airflow scales by active jobs |
| `PSE_SOURCE_POOL_SLOTS` | `6` | Shared Airflow source capacity |
| `PSE_EDGE_JOB_SLOTS` | `2` | Source-pool slots and workers reserved by each mapped job |
| `PSE_EDGE_JOBS` | Default current-state jobs | Optional comma-separated set of scheduled domain jobs |
| `PSE_EDGE_SEARCH_WINDOW_DAYS` | Job default; `0` means unbounded | Override the rolling date window for jobs with date filters |
| `PSE_EDGE_RSS_FEEDS` | empty | Optional RSS/Atom feed URLs hosted on the PSE EDGE domain |
| `PSE_ETL_WORKERS` | `4` | Concurrent raw-object reads and transforms |
| `S3_BUCKET_RAW` | `raw` | Immutable responses and source manifests |
| `S3_BUCKET_STAGE` | `stage` | Normalized Parquet objects and ETL manifests |
| `S3_BUCKET_CURATED` | `curated` | Deduplicated Parquet objects |
| `S3_BUCKET_QUARANTINE` | `quarantine` | Transformation diagnostics |
| `S3_BUCKET_LOGS` | `airflow-logs` | Airflow remote task logs |
| `WAREHOUSE_DATABASE_URL` | Compose warehouse URL | PostgreSQL for non-Airflow callers |
| `AIRFLOW_CONN_PSE_WAREHOUSE` | Compose warehouse connection | PostgresHook connection for warehouse loads |

To run a full local backfill, use `python -m tools.pse_edge_fetch --scope configured_sources` with object-store configuration. The CLI also accepts `all` (the same full scope), any named job, and `rss`; add `--full-history` when backfilling a named date-filtered job. Through Airflow, use `{"jobs": ["all"]}` for separate domain pipelines or `{"jobs": ["configured_sources"]}` for a single full-catalog scope. A successful manifest records captured routes, source counts, failures, truncation, and coverage notes.

## Libraries and orchestration

- Scrapling provides pooled HTTP sessions; Polars and Pandera handle normalized frames and contracts.
- Pydantic validates the source manifest; boto3 provides object storage; SQLAlchemy and Airflow's PostgresHook handle warehouse writes.
- Airflow dynamic task mapping provides independent job retries and logs. Pools coordinate aggregate source pressure; capture and transform tasks use separate Celery queues.
- dbt Core and the Postgres adapter build analytics models. The asset-aware DAG starts dbt after warehouse loads succeed.

Airflow logs include run IDs, manifest locations, row counts, sink outcomes, and exception traces. dbt output is forwarded to the task log while the subprocess runs.

## Example queries

```sql
SELECT dataset, count(*)
FROM pse_edge.records
GROUP BY dataset
ORDER BY dataset;

SELECT payload
FROM pse_edge.records
WHERE dataset = 'company_announcements'
ORDER BY last_seen_at DESC
LIMIT 20;

SELECT *
FROM pse_edge.ingestion_runs
ORDER BY completed_at DESC
LIMIT 20;
```

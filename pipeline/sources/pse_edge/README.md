# PSE EDGE source

This connector captures public PSE EDGE pages, search endpoints, linked disclosure files, and the configured RSS/Atom feeds. The full crawl covers the public portal catalog in `catalog.py`; it does not include licensed exchange feeds or endpoints requiring separate authorization.

## Run lanes

- `pse_edge_full_ingestion` runs weekdays at 03:15 Asia/Manila. It captures catalog pages, paginated searches, company profiles, price history, and discovered documents.
- `pse_edge_rss_ingestion` polls the URLs in `PSE_RSS_FEEDS` every 15 minutes. Airflow reads the list at task runtime and dynamically maps one independently retryable capture task per distinct URL. A feed with a required fetch failure retries on its own and keeps its diagnostic manifest in raw storage; the ETL reducer waits for all mapped feeds to succeed. It combines the small manifest references into one ETL input, so payloads stay in object storage and do not travel through XCom. The variable accepts a comma-separated list; the default is `https://www.pse.com.ph/feed/`.
- `pse_edge_analytics` runs dbt when either source's successful warehouse load emits its warehouse asset, including the `pse_website` source.

Search, profile, price-history, and document limits are configurable. Search history is bounded by `PSE_MAX_SEARCH_PAGES`; increase it for deeper backfills. The `pse_edge_source` Airflow pool is shared across both ingestion DAGs and defaults to six slots. A full crawl occupies the pool capacity and caps its internal request workers to that capacity; each mapped RSS task uses one slot and one HTTP worker, so multiple feeds can run concurrently while the full crawl and RSS lane remain mutually coordinated. Set `PSE_SOURCE_POOL_SLOTS` to change the shared capacity. Within each task, the HTTP client uses persistent sessions, retries for transient errors, and a shared request-start delay.

## Source and ETL boundaries

`PSEEdgeConnector` implements the shared `SourceConnector` contract. It returns a `ManifestRef` that identifies an immutable source manifest. Source connectors can be added for databases, APIs, or other systems by implementing that contract and writing their raw inputs through `ObjectStore`.

The ETL runner reads the common manifest envelope and hands each raw object to a source-specific `Transformer`. Transformation and validation are separated from loading. The normalized record frame is checked with Pandera and represented in Polars. `DatasetRouter` chooses destinations by dataset and record type; `DataSink` adapters implement those destinations independently. The included sinks write Parquet to S3-compatible object storage, load selected records into PostgreSQL, and write transformation diagnostics to quarantine storage. A future document store, NoSQL database, or image store can be added as a sink without changing the PSE scraper.

## Data destinations

| Data | Destination | Notes |
| --- | --- | --- |
| Original HTML, JSON, RSS/XML, PDFs, images, videos, and other attachments | `raw` object-store bucket | Immutable capture; binary assets remain object bytes rather than SQL blobs |
| Normalized records | `stage` object-store bucket | Zstandard Parquet, partitioned by source, dataset, and run |
| Deduplicated records | `curated` object-store bucket | Parquet with one latest row per `(dataset, record_key)` in that run |
| Searchable business records and ingestion audit | PostgreSQL `pse_edge` schema | JSONB payload plus stable record key, source object reference, and first/last-seen timestamps |
| Transform errors | `quarantine` object-store bucket | JSON diagnostics retained for repair or replay |
| Airflow task logs | `airflow-logs` object-store bucket | Airflow's Amazon provider S3 task handler; local task log copies are retained |
| Analytics models | PostgreSQL `analytics` schema | dbt views over the warehouse tables |

`DatasetRouter` currently sends disclosure-asset metadata and binary-asset metadata to object storage only. The connector follows PSE-hosted document, image, and video links; their original bytes remain in the raw bucket. Other normalized datasets go to Parquet and PostgreSQL. Dataset and record-type restrictions are combined, so a broad record-type route cannot override a narrower dataset rule.

## Libraries and Airflow providers

- Scrapling provides the HTTP sessions used by the connector.
- Polars builds and writes Parquet record frames; Pandera validates the normalized frame contract.
- Pydantic validates the source-manifest envelope.
- boto3 provides pooled S3-compatible object-store I/O; SQLAlchemy provides bounded PostgreSQL upserts.
- dbt Core and the Postgres adapter build analytics models from warehouse tables.
- Airflow's Celery, Postgres, Amazon, and FAB providers support distributed task execution, warehouse connections through `PostgresHook`, remote S3 task logs, and the configured auth manager. Airflow dynamic task mapping provides per-feed retries and logs; asset-aware scheduling starts dbt only after a warehouse load succeeds. Source and transformation work use separate Celery queues, and Airflow pools coordinate source pressure across DAGs.

Airflow task functions use the task logger and include run IDs, manifest locations, row counts, sink outcomes, and exception traces. dbt output is forwarded line by line while the command runs, so it appears in the Airflow task log instead of being buffered until the subprocess exits.

The current PSE connector uses static pages and public HTTP endpoints, so it runs on the `scrape-light` queue. The browser worker is disabled by default and can be started with the `browser-scraping` Compose profile when a future source needs JavaScript rendering.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `PSE_EDGE_BASE_URL` | `https://edge.pse.com.ph` | Public portal base URL |
| `PSE_RSS_FEEDS` | `https://www.pse.com.ph/feed/` | Comma-separated RSS or Atom feed URLs |
| `PSE_MAX_SEARCH_PAGES` | `100` | Page cap per search endpoint |
| `PSE_MAX_COMPANY_PROFILES` | `500` | Company-profile cap per full crawl |
| `PSE_MAX_PRICE_HISTORIES_PER_RUN` | `500` | Security history cap per full crawl |
| `PSE_PRICE_HISTORY_START_DATE` | `2000-01-01` | Start date for historical price requests |
| `PSE_MAX_DOCUMENTS_PER_RUN` | `100` | Disclosure viewer or attachment cap per full crawl |
| `PSE_EDGE_CONCURRENCY` | `6` | Maximum concurrent HTTP request workers inside one capture, bounded to 1–16 and the source-pool size |
| `PSE_SOURCE_POOL_SLOTS` | `6` | Shared Airflow source capacity across full and RSS capture tasks |
| `PSE_REQUEST_DELAY_SECONDS` | `0.5` | Minimum delay between request starts inside each crawl |
| `PSE_ETL_WORKERS` | `4` | Concurrent raw-object reads and transforms, bounded to 1–16 |
| `S3_BUCKET_RAW` | `raw` | Immutable source objects and source manifests |
| `S3_BUCKET_STAGE` | `stage` | Normalized Parquet objects and ETL manifests |
| `S3_BUCKET_CURATED` | `curated` | Deduplicated Parquet objects |
| `S3_BUCKET_QUARANTINE` | `quarantine` | Transformation error diagnostics |
| `S3_BUCKET_LOGS` | `airflow-logs` | Airflow remote task logs |
| `WAREHOUSE_DATABASE_URL` | Compose warehouse URL | PostgreSQL connection for non-Airflow pipeline callers |
| `AIRFLOW_CONN_PSE_WAREHOUSE` | Compose warehouse connection | Airflow PostgresHook connection used by the warehouse load task |

For a single raw capture, run `python -m tools.pse_edge_fetch` from the repository root with the object-store environment configured.

## Warehouse queries

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

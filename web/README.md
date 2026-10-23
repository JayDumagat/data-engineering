# PSE Data Observatory

The Next.js App Router application provides a read-only interface to the warehouse schemas populated by the PSE EDGE and PSE Website pipelines. It displays dataset coverage, recent ingestion runs, searchable records, source links, and the stored raw-object pointer.

## Run with Docker Compose

From the repository root:

```powershell
docker compose up -d --build data-viewer
```

Open <http://localhost:3000>. Set `DATA_VIEWER_PORT` in `.env` to use a different host port. The viewer waits for `warehouse-postgres` to pass its health check and receives its connection settings from the existing warehouse environment variables.

## Run locally for development

Start the warehouse, then from this directory:

```powershell
npm install
$env:PGHOST = "localhost"
$env:PGPORT = "5433"
$env:PGDATABASE = $env:WAREHOUSE_DB
$env:PGUSER = $env:WAREHOUSE_USER
$env:PGPASSWORD = $env:WAREHOUSE_PASSWORD
npm run dev
```

The local port defaults to 3000. The development server expects the repository `.env` values to be available in the shell or copied into the corresponding `PG*` variables.

## Data access

The server-side API uses a bounded PostgreSQL connection pool and only selects from the fixed `pse_edge` and `pse_website` schema names. Search, dataset, record-type, and pagination values are parameterized. The browser never receives database credentials. API responses are not cached because ingestion can update the warehouse between page views.

The viewer reads `records` and `ingestion_runs`; raw files remain in SeaweedFS and are represented by their bucket/key in the record detail panel.

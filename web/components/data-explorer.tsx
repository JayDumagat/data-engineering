"use client";

import { useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";

type SourceKey = "pse_edge" | "pse_website";
type DatasetCount = { name: string; count: number };
type Run = {
  run_id: string;
  scope: string;
  status: string;
  started_at: string;
  completed_at: string;
  raw_object_count: number;
  stage_record_count: number;
  curated_record_count: number;
  failures: unknown[];
};
type Overview = {
  source: SourceKey;
  sourceLabel: string;
  ready: boolean;
  totalRecords: number;
  latestSeenAt: string | null;
  datasets: DatasetCount[];
  recordTypes: DatasetCount[];
  runs: Run[];
};
type RecordRow = {
  record_id: string;
  dataset: string;
  record_type: string;
  record_key: string;
  source_url: string;
  raw_bucket: string;
  raw_key: string;
  title: string | null;
  category: string | null;
  company_name: string | null;
  symbol: string | null;
  published_at: string | null;
  report_type: string | null;
  report_period: string | null;
  report_date: string | null;
  uploaded_date: string | null;
  detail_url: string | null;
  download_url: string | null;
  statistic_name: string | null;
  year: number | null;
  main_value: number | string | null;
  sme_value: number | string | null;
  total_value: number | string | null;
  payload: Record<string, unknown>;
  first_seen_at: string;
  last_seen_at: string;
};
type RecordResponse = { rows: RecordRow[]; total: number; page: number; pageSize: number; ready: boolean };

const EMPTY_OVERVIEW: Overview = {
  source: "pse_edge",
  sourceLabel: "PSE EDGE",
  ready: false,
  totalRecords: 0,
  latestSeenAt: null,
  datasets: [],
  recordTypes: [],
  runs: [],
};

function formatCount(value: number) {
  return new Intl.NumberFormat("en-PH").format(value);
}

function formatDate(value: string | null | undefined) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.valueOf())
    ? "—"
    : new Intl.DateTimeFormat("en-PH", { dateStyle: "medium", timeStyle: "short" }).format(date);
}

function titleFor(row: RecordRow) {
  if (isUsefulTitle(row.title) && !isNumericValue(row.title)) return row.title.trim();
  const payload = row.payload ?? {};
  for (const key of ["title", "name", "company_name", "announcement_title", "subject", "symbol"]) {
    const value = payload[key];
    if (isUsefulTitle(value) && !isNumericValue(value)) return String(value).trim();
  }
  const columns = payload.columns;
  if (columns && typeof columns === "object" && !Array.isArray(columns)) {
    const entries = Object.entries(columns as Record<string, unknown>);
    const titleKeys = [
      /company|issuer|security/i,
      /symbol|ticker/i,
      /index|statistic|metric/i,
      /title|subject|event|report|name/i,
      /record.?date|ex.?date|date/i,
    ];
    for (const pattern of titleKeys) {
      const match = entries.find(([key, value]) => pattern.test(key) && isUsefulTitle(value) && !isNumericValue(value));
      if (match) return String(match[1]).trim();
    }
    const firstLabel = entries.find(([, value]) => isUsefulTitle(value) && !isNumericValue(value));
    if (firstLabel) return String(firstLabel[1]).trim();
  }
  if (row.dataset === "index_summary") return "Index summary metric";
  return row.record_key;
}

function isUsefulTitle(value: unknown): value is string {
  if (typeof value !== "string" || !value.trim()) return false;
  return !/^(?:n\/?a|tba|no data(?: available)?|no (?:records?|results?) found|[-–—]+)[.!:]?$/i.test(value.trim());
}

function isNumericValue(value: unknown): boolean {
  return typeof value === "string" && /^[+-]?(?:\d[\d,]*(?:\.\d+)?%?|\.\d+%?)(?:[▲▼↑↓])?$/.test(value.trim());
}

function shortId(value: string) {
  return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-7)}` : value;
}

function Icon({ name }: { name: "grid" | "database" | "activity" | "search" | "refresh" | "arrow" | "external" | "close" }) {
  const common = { width: 18, height: 18, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.8, strokeLinecap: "round" as const, strokeLinejoin: "round" as const, "aria-hidden": true as const };
  const paths = {
    grid: <><rect x="3" y="3" width="7" height="7" rx="1" /><rect x="14" y="3" width="7" height="7" rx="1" /><rect x="3" y="14" width="7" height="7" rx="1" /><rect x="14" y="14" width="7" height="7" rx="1" /></>,
    database: <><ellipse cx="12" cy="5" rx="8" ry="3" /><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5" /><path d="M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3" /></>,
    activity: <><path d="M3 12h4l3-8 4 16 3-8h4" /></>,
    search: <><circle cx="10.8" cy="10.8" r="6.8" /><path d="m16 16 4.5 4.5" /></>,
    refresh: <><path d="M20 7v5h-5" /><path d="M4.8 9A7.5 7.5 0 0 1 18 6l2 2M4 17v-5h5" /><path d="M19.2 15A7.5 7.5 0 0 1 6 18l-2-2" /></>,
    arrow: <><path d="M5 12h14" /><path d="m13 6 6 6-6 6" /></>,
    external: <><path d="M14 4h6v6" /><path d="m20 4-9 9" /><path d="M18 13v6a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h6" /></>,
    close: <><path d="m18 6-12 12M6 6l12 12" /></>,
  };
  return <svg {...common}>{paths[name]}</svg>;
}

export function DataExplorer() {
  const [source, setSource] = useState<SourceKey>("pse_edge");
  const [overview, setOverview] = useState<Overview>(EMPTY_OVERVIEW);
  const [records, setRecords] = useState<RecordResponse>({ rows: [], total: 0, page: 1, pageSize: 25, ready: false });
  const [searchDraft, setSearchDraft] = useState("");
  const [search, setSearch] = useState("");
  const [dataset, setDataset] = useState("");
  const [recordType, setRecordType] = useState("");
  const [page, setPage] = useState(1);
  const [selected, setSelected] = useState<RecordRow | null>(null);
  const [overviewLoading, setOverviewLoading] = useState(true);
  const [recordsLoading, setRecordsLoading] = useState(true);
  const [error, setError] = useState("");
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    async function loadOverview() {
      setOverviewLoading(true);
      setError("");
      try {
        const response = await fetch(`/api/overview?source=${source}`, { cache: "no-store", signal: controller.signal });
        const body = await response.json();
        if (!response.ok) throw new Error(body.error ?? "Unable to load the warehouse overview.");
        setOverview(body as Overview);
      } catch (cause) {
        if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : "Unable to load the warehouse overview.");
      } finally {
        if (!controller.signal.aborted) setOverviewLoading(false);
      }
    }
    void loadOverview();
    return () => controller.abort();
  }, [source, refreshKey]);

  useEffect(() => {
    const controller = new AbortController();
    async function loadRecords() {
      setRecordsLoading(true);
      const params = new URLSearchParams({ source, page: String(page), pageSize: "25" });
      if (search) params.set("q", search);
      if (dataset) params.set("dataset", dataset);
      if (recordType) params.set("type", recordType);
      try {
        const response = await fetch(`/api/records?${params}`, { cache: "no-store", signal: controller.signal });
        const body = await response.json();
        if (!response.ok) throw new Error(body.error ?? "Unable to load records.");
        setRecords(body as RecordResponse);
      } catch (cause) {
        if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : "Unable to load records.");
      } finally {
        if (!controller.signal.aborted) setRecordsLoading(false);
      }
    }
    void loadRecords();
    return () => controller.abort();
  }, [source, search, dataset, recordType, page, refreshKey]);

  const maxDatasetCount = useMemo(() => Math.max(1, ...overview.datasets.map((item) => item.count)), [overview.datasets]);
  const pageCount = Math.max(1, Math.ceil(records.total / records.pageSize));
  const newestRun = overview.runs[0];
  const normalizedFields = selected ? [
    ["Category", selected.category],
    ["Company", selected.company_name],
    ["Symbol", selected.symbol],
    ["Published", selected.published_at],
    ["Report type", selected.report_type],
    ["Report period", selected.report_period],
    ["Report date", selected.report_date],
    ["Uploaded", selected.uploaded_date],
    ["Statistic", selected.statistic_name],
    ["Year", selected.year],
    ["Main", selected.main_value],
    ["SME", selected.sme_value],
    ["Total", selected.total_value],
  ].filter(([, value]) => value !== null && value !== undefined && String(value).trim()) : [];

  function changeSource(value: SourceKey) {
    setSource(value);
    setOverview({ ...EMPTY_OVERVIEW, source: value, sourceLabel: value === "pse_edge" ? "PSE EDGE" : "PSE Website" });
    setRecords({ rows: [], total: 0, page: 1, pageSize: 25, ready: false });
    setDataset("");
    setRecordType("");
    setSearch("");
    setSearchDraft("");
    setPage(1);
    setSelected(null);
  }

  function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setPage(1);
    setSearch(searchDraft.trim());
  }

  function clearFilters() {
    setDataset("");
    setRecordType("");
    setSearch("");
    setSearchDraft("");
    setPage(1);
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a className="brand" href="#overview" aria-label="PSE Data Observatory home">
          <span className="brand-mark"><span /></span>
          <span className="brand-copy"><strong>MarketScope</strong><small>DATA OBSERVATORY</small></span>
        </a>
        <div className="side-label">WORKSPACE</div>
        <nav className="side-nav" aria-label="Main navigation">
          <a className="nav-link active" href="#overview"><Icon name="grid" /> Overview</a>
          <a className="nav-link" href="#explorer"><Icon name="database" /> Data explorer</a>
          <a className="nav-link" href="#runs"><Icon name="activity" /> Ingestion runs</a>
        </nav>
        <div className="sidebar-bottom">
          <div className="storage-card">
          <div className="storage-heading"><span className={`storage-dot ${error ? "offline" : ""}`} />{error ? "Warehouse issue" : overviewLoading ? "Connecting" : "Warehouse"}</div>
          <p>{error ? "The PostgreSQL warehouse could not be reached." : "Connected to your local PostgreSQL warehouse."}</p>
          <span className="storage-status">{error ? "CONNECTION ERROR" : overviewLoading ? "CHECKING CONNECTION" : "LIVE CONNECTION"}</span>
          </div>
          <div className="sidebar-foot"><span className="mini-brand">MS</span><span>Philippine Stock Exchange<br /><small>Public data workspace</small></span></div>
        </div>
      </aside>

      <main className="main-content">
        <header className="topbar">
          <div className="breadcrumbs"><span>Workspace</span><span className="crumb-sep">/</span><strong>Overview</strong></div>
          <div className="top-actions">
            <span className={`top-status ${error ? "offline" : ""}`}><i /> {error ? "Warehouse unavailable" : overviewLoading ? "Connecting to warehouse" : "Warehouse online"}</span>
            <button className="icon-button" onClick={() => setRefreshKey((value) => value + 1)} aria-label="Refresh data" title="Refresh data"><Icon name="refresh" /></button>
            <span className="avatar">PS</span>
          </div>
        </header>

        <div className="content-wrap">
          <section className="page-heading" id="overview">
            <div>
              <div className="eyebrow">DATA WORKSPACE <span>•</span> PHILIPPINE STOCK EXCHANGE</div>
              <h1>Market data, in focus.</h1>
              <p>Explore collected public data and monitor your ingestion pipeline.</p>
            </div>
            <label className="source-select-wrap">
              <span>DATA SOURCE</span>
              <select value={source} onChange={(event) => changeSource(event.target.value as SourceKey)} aria-label="Data source">
                <option value="pse_edge">PSE EDGE</option>
                <option value="pse_website">PSE Website</option>
              </select>
              <span className="select-chevron">⌄</span>
            </label>
          </section>

          {error && <div className="error-banner" role="alert"><span>{error}</span><button onClick={() => setRefreshKey((value) => value + 1)}>Retry</button></div>}

          <section className="metrics-grid" aria-label="Warehouse metrics">
            <article className="metric-card metric-primary">
              <div className="metric-top"><span className="metric-label">TOTAL RECORDS</span><span className="metric-icon"><Icon name="database" /></span></div>
              <div className="metric-value">{formatCount(overview.totalRecords)}</div>
              <div className="metric-foot"><span className="metric-indicator" /> Records available in {overview.sourceLabel}</div>
            </article>
            <article className="metric-card">
              <div className="metric-top"><span className="metric-label">DATASETS</span><span className="metric-icon lavender"><Icon name="grid" /></span></div>
              <div className="metric-value">{formatCount(overview.datasets.length)}</div>
              <div className="metric-foot">Distinct collections in the warehouse</div>
            </article>
            <article className="metric-card">
              <div className="metric-top"><span className="metric-label">LAST UPDATED</span><span className="metric-icon amber"><Icon name="refresh" /></span></div>
              <div className="metric-date">{overview.latestSeenAt ? formatDate(overview.latestSeenAt) : "No updates yet"}</div>
              <div className="metric-foot">Most recently observed source record</div>
            </article>
            <article className="metric-card">
              <div className="metric-top"><span className="metric-label">LATEST RUN</span><span className="metric-icon blue"><Icon name="activity" /></span></div>
              <div className="metric-value metric-run">{newestRun ? <><span className={`run-dot ${newestRun.status.toLowerCase()}`} />{newestRun.status}</> : "—"}</div>
              <div className="metric-foot">{newestRun ? formatDate(newestRun.completed_at) : "No ingestion runs recorded"}</div>
            </article>
          </section>

          <section className="insights-grid">
            <article className="panel dataset-panel">
              <div className="panel-heading"><div><h2>Dataset coverage</h2><p>Records grouped by collection</p></div><a href="#explorer" className="text-link">Explore data <Icon name="arrow" /></a></div>
              {overview.datasets.length ? <div className="dataset-list">
                {overview.datasets.slice(0, 6).map((item, index) => <div className="dataset-row" key={item.name}>
                  <div className="dataset-meta"><span className={`dataset-bullet bullet-${index % 5}`} /><span className="dataset-name" title={item.name}>{item.name}</span><strong>{formatCount(item.count)}</strong></div>
                  <div className="bar-track"><div className={`bar-fill fill-${index % 5}`} style={{ width: `${Math.max(3, (item.count / maxDatasetCount) * 100)}%` }} /></div>
                </div>)}
              </div> : <div className="panel-empty"><span className="empty-icon"><Icon name="database" /></span><strong>{overviewLoading ? "Loading dataset coverage" : "No datasets yet"}</strong><span>Collections appear here as records are loaded into the warehouse.</span></div>}
            </article>

            <article className="panel activity-panel" id="runs">
              <div className="panel-heading"><div><h2>Recent ingestion</h2><p>Latest pipeline activity</p></div><span className="live-pill"><i /> LIVE</span></div>
              {overview.runs.length ? <div className="run-list">
                {overview.runs.slice(0, 5).map((run) => <div className="run-item" key={run.run_id}>
                  <span className={`run-icon ${run.status.toLowerCase()}`}><Icon name="activity" /></span>
                  <div className="run-copy"><div><strong>{run.scope}</strong><span className={`status-pill ${run.status.toLowerCase()}`}>{run.status}</span></div><small>{formatDate(run.completed_at)}</small></div>
                  <div className="run-count"><strong>{formatCount(run.curated_record_count)}</strong><small>records</small></div>
                </div>)}
              </div> : <div className="panel-empty compact"><span className="empty-icon"><Icon name="activity" /></span><strong>No pipeline runs yet</strong><span>Run history will show here after the first ingestion.</span></div>}
            </article>
          </section>

          <section className="panel explorer-panel" id="explorer">
            <div className="explorer-heading"><div><div className="section-kicker">WAREHOUSE</div><h2>Data explorer</h2><p>Search and inspect captured records from {overview.sourceLabel}.</p></div><span className="record-total"><strong>{formatCount(records.total)}</strong> records</span></div>
            <form className="filter-toolbar" onSubmit={submitSearch}>
              <label className="search-field"><Icon name="search" /><input value={searchDraft} onChange={(event) => setSearchDraft(event.target.value)} placeholder="Search records, titles, or source URLs" aria-label="Search records" /><button type="submit">Search</button></label>
              <label className="filter-select"><span className="sr-only">Dataset</span><select value={dataset} onChange={(event) => { setDataset(event.target.value); setPage(1); }}><option value="">All datasets</option>{overview.datasets.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}</select></label>
              <label className="filter-select type-filter"><span className="sr-only">Record type</span><select value={recordType} onChange={(event) => { setRecordType(event.target.value); setPage(1); }}><option value="">All record types</option>{overview.recordTypes.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}</select></label>
              {(search || dataset || recordType) && <button type="button" className="clear-button" onClick={clearFilters}>Clear</button>}
            </form>
            <div className="table-scroll">
              <table className="records-table">
                <thead><tr><th>RECORD</th><th>DATASET</th><th>TYPE</th><th>SOURCE</th><th>LAST SEEN</th><th aria-label="Open record" /></tr></thead>
                <tbody>
                  {records.rows.map((row) => <tr key={row.record_id} onClick={() => setSelected(row)} tabIndex={0} onKeyDown={(event) => { if (event.key === "Enter") setSelected(row); }}>
                    <td><div className="record-title">{titleFor(row)}</div><div className="record-key">{shortId(row.record_key)}</div></td>
                    <td><span className="dataset-tag">{row.dataset}</span></td>
                    <td><span className="type-text">{row.record_type || "—"}</span></td>
                    <td><a className="source-link" href={row.source_url} target="_blank" rel="noreferrer" onClick={(event) => event.stopPropagation()} title={row.source_url}><span>{row.source_url.replace(/^https?:\/\//, "").replace(/\/$/, "")}</span><Icon name="external" /></a></td>
                    <td><span className="date-cell">{formatDate(row.last_seen_at)}</span></td>
                    <td><button className="row-open" aria-label={`Inspect ${titleFor(row)}`} onClick={(event) => { event.stopPropagation(); setSelected(row); }}><Icon name="arrow" /></button></td>
                  </tr>)}
                </tbody>
              </table>
              {!records.rows.length && <div className="table-empty"><span className="empty-icon"><Icon name="search" /></span><strong>{recordsLoading ? "Loading records" : overview.ready ? "No matching records" : "Warehouse is ready for data"}</strong><span>{overview.ready ? "Try changing your filters or search terms." : "Start an ingestion DAG to populate this view."}</span></div>}
            </div>
            <div className="table-footer"><span>Showing {records.total ? `${formatCount((page - 1) * records.pageSize + 1)}–${formatCount(Math.min(page * records.pageSize, records.total))}` : "0"} of {formatCount(records.total)} records</span><div className="pagination"><button disabled={page <= 1 || recordsLoading} onClick={() => setPage((value) => Math.max(1, value - 1))}>Previous</button><span>Page <strong>{page}</strong> of {pageCount}</span><button disabled={page >= pageCount || recordsLoading} onClick={() => setPage((value) => Math.min(pageCount, value + 1))}>Next <Icon name="arrow" /></button></div></div>
          </section>
          <footer className="page-footer"><span>MarketScope <span>•</span> PSE data observatory</span><span>Data stored in your local warehouse</span></footer>
        </div>
      </main>

      {selected && <div className="drawer-backdrop" role="presentation" onClick={() => setSelected(null)}>
        <aside className="detail-drawer" role="dialog" aria-modal="true" aria-labelledby="detail-title" onClick={(event) => event.stopPropagation()}>
          <div className="drawer-header"><div><span className="section-kicker">RECORD DETAIL</span><h2 id="detail-title">{titleFor(selected)}</h2></div><button className="icon-button" onClick={() => setSelected(null)} aria-label="Close record details"><Icon name="close" /></button></div>
          <div className="detail-meta"><div><span>DATASET</span><strong>{selected.dataset}</strong></div><div><span>RECORD TYPE</span><strong>{selected.record_type || "Unknown"}</strong></div><div><span>LAST SEEN</span><strong>{formatDate(selected.last_seen_at)}</strong></div><div><span>FIRST SEEN</span><strong>{formatDate(selected.first_seen_at)}</strong></div></div>
          {normalizedFields.length > 0 && <><div className="payload-heading"><h3>Normalized fields</h3><span>WAREHOUSE</span></div><div className="detail-meta">{normalizedFields.map(([label, value]) => <div key={String(label)}><span>{label}</span><strong>{String(value)}</strong></div>)}</div></>}
          {(selected.detail_url || selected.download_url) && <div className="detail-meta">{selected.detail_url && <div><span>DETAIL PAGE</span><strong><a href={selected.detail_url} target="_blank" rel="noreferrer">Open source record</a></strong></div>}{selected.download_url && <div><span>DOWNLOAD</span><strong><a href={selected.download_url} target="_blank" rel="noreferrer">Open source file</a></strong></div>}</div>}
          <a className="detail-source" href={selected.source_url} target="_blank" rel="noreferrer"><span><small>ORIGINAL SOURCE</small>{selected.source_url}</span><Icon name="external" /></a>
          <div className="raw-pointer"><span>RAW OBJECT</span><code>{selected.raw_bucket}/{selected.raw_key}</code></div>
          <div className="payload-heading"><h3>Payload</h3><span>JSON</span></div>
          <pre className="payload-view">{JSON.stringify(selected.payload, null, 2)}</pre>
        </aside>
      </div>}
    </div>
  );
}

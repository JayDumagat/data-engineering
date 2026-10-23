import { Pool } from "pg";

export const SOURCES = {
  pse_edge: { label: "PSE EDGE", schema: "pse_edge" },
  pse_website: { label: "PSE Website", schema: "pse_website" },
} as const;
export const EDGE_PRESENTATION_TABLES = ["advanced_search_page", "contact_us", "etf_search_page"];

export function displayableRecordsSql(recordTypeField: string) {
  const hasMeaningfulTableCell = `exists (
    select 1 from (
      select value from jsonb_each_text(case when jsonb_typeof(payload -> 'columns') = 'object' then payload -> 'columns' else '{}'::jsonb end)
      union all
      select value from jsonb_array_elements_text(case when jsonb_typeof(payload -> 'cells') = 'array' then payload -> 'cells' else '[]'::jsonb end)
    ) as table_values
    where lower(trim(regexp_replace(table_values.value, '[.!:]+$', ''))) not in ('', '-', '--', '—', '–', 'n/a', 'na', 'not available', 'tba', 'no data', 'no data available', 'no record', 'no records', 'no records found', 'no result', 'no results', 'no results found', 'no matching records', 'null', 'none')
  )`;
  return `${recordTypeField} not in ('page_snapshot', 'source_table_row', 'outbound_link', 'sitemap_url', 'robots_policy', 'crawl_summary') and record_key <> '' and payload <> '{}'::jsonb and not (${recordTypeField} = 'table_row' and not ${hasMeaningfulTableCell})`;
}

export type SourceKey = keyof typeof SOURCES;

declare global {
  // eslint-disable-next-line no-var
  var __pseDataPool: Pool | undefined;
}

function createPool() {
  return new Pool({
    host: process.env.PGHOST ?? "warehouse-postgres",
    port: Number(process.env.PGPORT ?? 5432),
    database: process.env.PGDATABASE ?? "warehouse",
    user: process.env.PGUSER ?? "warehouse",
    password: process.env.PGPASSWORD,
    max: 8,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
    application_name: "pse-data-observatory",
  });
}

export const pool = globalThis.__pseDataPool ?? createPool();
if (process.env.NODE_ENV !== "production") globalThis.__pseDataPool = pool;

export function sourceKey(value: string | null): SourceKey {
  return value === "pse_website" ? "pse_website" : "pse_edge";
}

export async function sourceTablesExist(source: SourceKey) {
  const schema = SOURCES[source].schema;
  const result = await pool.query<{
    records_table: string | null;
    runs_table: string | null;
    normalized_records: boolean;
  }>(
    `select to_regclass($1) as records_table,
            to_regclass($2) as runs_table,
            exists (
              select 1 from information_schema.columns
              where table_schema = $3 and table_name = 'records' and column_name = 'record_type'
            ) as normalized_records`,
    [`${schema}.records`, `${schema}.ingestion_runs`, schema],
  );
  return {
    records: result.rows[0]?.records_table !== null && result.rows[0]?.records_table !== undefined,
    runs: result.rows[0]?.runs_table !== null && result.rows[0]?.runs_table !== undefined,
    normalizedRecords: result.rows[0]?.normalized_records ?? false,
  };
}


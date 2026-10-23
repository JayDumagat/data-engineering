import { NextRequest, NextResponse } from "next/server";
import { displayableRecordsSql, EDGE_PRESENTATION_TABLES, pool, sourceKey, sourceTablesExist, SOURCES } from "@/lib/db";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const WEBSITE_DATASETS = ["market_reports", "announcements", "press_releases", "listing_statistics", "website_feeds"];
const WEBSITE_RECORD_TYPES = ["market_report", "announcement", "press_release", "listing_statistic", "rss_item"];

export async function GET(request: NextRequest) {
  const source = sourceKey(request.nextUrl.searchParams.get("source"));
  const schema = SOURCES[source].schema;

  try {
    const tables = await sourceTablesExist(source);
    if (!tables.records) {
      return NextResponse.json({
        source,
        sourceLabel: SOURCES[source].label,
        ready: false,
        totalRecords: 0,
        latestSeenAt: null,
        datasets: [],
        recordTypes: [],
        runs: [],
      }, { headers: { "Cache-Control": "no-store" } });
    }

    const recordTypeField = tables.normalizedRecords
      ? "coalesce(record_type, 'unknown')"
      : "coalesce(payload ->> 'record_type', 'unknown')";
    const displayableRecords = displayableRecordsSql(recordTypeField);
    const datasetFilter = source === "pse_website"
      ? `dataset = any($1::text[]) and ${recordTypeField} = any($2::text[]) and `
      : source === "pse_edge"
        ? `(dataset <> 'rss' or source_url like 'https://edge.pse.com.ph/%') and not (dataset = any($1::text[]) and ${recordTypeField} = 'table_row') and `
        : "";
    const filterValues = source === "pse_website"
      ? [WEBSITE_DATASETS, WEBSITE_RECORD_TYPES]
      : source === "pse_edge" ? [EDGE_PRESENTATION_TABLES] : [];
    const runFilter = source === "pse_edge" ? "where scope <> 'rss' " : "";
    const [summary, datasets, recordTypes, runs] = await Promise.all([
      pool.query<{ total: string; latest_seen_at: Date | null }>(
        `select count(*)::text as total, max(last_seen_at) as latest_seen_at from ${schema}.records where ${datasetFilter}${displayableRecords}`,
        filterValues,
      ),
      pool.query<{ dataset: string; count: string }>(
        `select dataset, count(*)::text as count from ${schema}.records where ${datasetFilter}${displayableRecords} group by dataset order by count(*) desc, dataset asc`,
        filterValues,
      ),
      pool.query<{ record_type: string; count: string }>(
        `select ${recordTypeField} as record_type, count(*)::text as count
         from ${schema}.records where ${datasetFilter}${displayableRecords} group by 1 order by count(*) desc, record_type asc limit 100`,
        filterValues,
      ),
      tables.runs
        ? pool.query(
            `select run_id, scope, status, started_at, completed_at, raw_object_count,
                    stage_record_count, curated_record_count, failures
             from ${schema}.ingestion_runs ${runFilter}order by completed_at desc limit 8`,
          )
        : Promise.resolve({ rows: [] }),
    ]);

    return NextResponse.json({
      source,
      sourceLabel: SOURCES[source].label,
      ready: true,
      totalRecords: Number(summary.rows[0]?.total ?? 0),
      latestSeenAt: summary.rows[0]?.latest_seen_at ?? null,
      datasets: datasets.rows.map((row) => ({ name: row.dataset, count: Number(row.count) })),
      recordTypes: recordTypes.rows.map((row) => ({ name: row.record_type, count: Number(row.count) })),
      runs: runs.rows,
    }, { headers: { "Cache-Control": "no-store" } });
  } catch (error) {
    console.error("PSE dashboard overview query failed", error);
    return NextResponse.json({ error: "Warehouse query failed. Check that PostgreSQL is available." }, { status: 503, headers: { "Cache-Control": "no-store" } });
  }
}

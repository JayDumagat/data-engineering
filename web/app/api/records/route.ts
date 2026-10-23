import { NextRequest, NextResponse } from "next/server";
import { displayableRecordsSql, EDGE_PRESENTATION_TABLES, pool, sourceKey, sourceTablesExist, SOURCES } from "@/lib/db";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const WEBSITE_DATASETS = ["market_reports", "announcements", "press_releases", "listing_statistics", "website_feeds"];
const WEBSITE_RECORD_TYPES = ["market_report", "announcement", "press_release", "listing_statistic", "rss_item"];

export async function GET(request: NextRequest) {
  const params = request.nextUrl.searchParams;
  const source = sourceKey(params.get("source"));
  const schema = SOURCES[source].schema;
  const search = (params.get("q") ?? "").trim().slice(0, 160);
  const dataset = (params.get("dataset") ?? "").trim().slice(0, 120);
  const recordType = (params.get("type") ?? "").trim().slice(0, 120);
  const page = Math.min(10_000, Math.max(1, Number.parseInt(params.get("page") ?? "1", 10) || 1));
  const pageSize = Math.min(100, Math.max(10, Number.parseInt(params.get("pageSize") ?? "25", 10) || 25));

  try {
    const tables = await sourceTablesExist(source);
    if (!tables.records) {
      return NextResponse.json({ rows: [], total: 0, page, pageSize, ready: false }, { headers: { "Cache-Control": "no-store" } });
    }

    const recordTypeField = tables.normalizedRecords
      ? "coalesce(record_type, 'unknown')"
      : "coalesce(payload ->> 'record_type', 'unknown')";
    const titleField = tables.normalizedRecords ? "coalesce(title, '')" : "coalesce(payload ->> 'title', '')";
    const displayableRecords = displayableRecordsSql(recordTypeField);
    const clauses: string[] = [displayableRecords];
    const values: unknown[] = [];
    if (source === "pse_website") {
      values.push(WEBSITE_DATASETS);
      clauses.push(`dataset = any($${values.length}::text[])`);
      values.push(WEBSITE_RECORD_TYPES);
      clauses.push(`${recordTypeField} = any($${values.length}::text[])`);
    } else if (source === "pse_edge") {
      clauses.push("(dataset <> 'rss' or source_url like 'https://edge.pse.com.ph/%')");
      values.push(EDGE_PRESENTATION_TABLES);
      clauses.push(`not (dataset = any($${values.length}::text[]) and ${recordTypeField} = 'table_row')`);
    }
    const addFilter = (clause: (position: number) => string, value: unknown) => {
      values.push(value);
      clauses.push(clause(values.length));
    };
    if (dataset) addFilter((position) => `dataset = $${position}`, dataset);
    if (recordType) addFilter((position) => `${recordTypeField} = $${position}`, recordType);
    if (search) {
      addFilter(
        (position) => `strpos(lower(record_key || ' ' || dataset || ' ' || source_url || ' ' || ${titleField} || ' ' || payload::text), lower($${position})) > 0`,
        search,
      );
    }

    const where = clauses.length ? `where ${clauses.join(" and ")}` : "";
    const countQuery = pool.query<{ total: string }>(
      `select count(*)::text as total from ${schema}.records ${where}`,
      values,
    );
    const rowValues = [...values, pageSize, (page - 1) * pageSize];
    const typedProjection = tables.normalizedRecords
      ? `record_type, title, category, company_name, symbol, published_at, report_type,
         report_period, report_date, uploaded_date, detail_url, download_url,
         statistic_name, year, main_value, sme_value, total_value`
      : `payload ->> 'record_type' as record_type, payload ->> 'title' as title,
         payload ->> 'category' as category, payload ->> 'company_name' as company_name,
         payload ->> 'symbol' as symbol, payload ->> 'published_at' as published_at,
         payload ->> 'report_type' as report_type, payload ->> 'report_period' as report_period,
         payload ->> 'report_date' as report_date, payload ->> 'uploaded_date' as uploaded_date,
         payload ->> 'detail_url' as detail_url, payload ->> 'download_url' as download_url,
         payload ->> 'statistic_name' as statistic_name, payload ->> 'year' as year,
         payload ->> 'main' as main_value, payload ->> 'sme' as sme_value, payload ->> 'total' as total_value`;
    const rowsQuery = pool.query(
      `select record_id, dataset, ${typedProjection}, record_key, source_url, raw_bucket, raw_key, payload,
              first_seen_at, last_seen_at
       from ${schema}.records ${where}
       order by last_seen_at desc, dataset asc, record_key asc
       limit $${values.length + 1} offset $${values.length + 2}`,
      rowValues,
    );
    const [count, rows] = await Promise.all([countQuery, rowsQuery]);
    return NextResponse.json({
      rows: rows.rows,
      total: Number(count.rows[0]?.total ?? 0),
      page,
      pageSize,
      ready: true,
    }, { headers: { "Cache-Control": "no-store" } });
  } catch (error) {
    console.error("PSE dashboard records query failed", error);
    return NextResponse.json({ error: "Warehouse query failed. Check that PostgreSQL is available." }, { status: 503, headers: { "Cache-Control": "no-store" } });
  }
}

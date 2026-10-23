{{ config(materialized='table') }}

select
    record_id as edge_record_id,
    record_key,
    dataset,
    record_type,
    title,
    company_name,
    symbol,
    published_at,
    report_period,
    category,
    detail_url,
    download_url,
    source_url,
    raw_bucket,
    raw_key,
    payload,
    first_seen_at,
    last_seen_at
from {{ ref('stg_pse_edge_records') }}
where record_type not in ('page_snapshot', 'outbound_link', 'sitemap_url', 'robots_policy', 'crawl_summary')
  and record_type <> 'source_table_row'
  and not (
      dataset in ('advanced_search_page', 'contact_us', 'etf_search_page')
      and record_type = 'table_row'
  )
  and not (
      record_type = 'table_row'
      and not exists (
          select 1 from (
              select value from jsonb_each_text(
                  case when jsonb_typeof(payload -> 'columns') = 'object' then payload -> 'columns' else '{}'::jsonb end
              )
              union all
              select value from jsonb_array_elements_text(
                  case when jsonb_typeof(payload -> 'cells') = 'array' then payload -> 'cells' else '[]'::jsonb end
              )
          ) as table_values
          where lower(trim(regexp_replace(table_values.value, '[.!:]+$', ''))) not in (
              '', '-', '--', '—', '–', 'n/a', 'na', 'not available', 'tba',
              'no data', 'no data available', 'no record', 'no records',
              'no records found', 'no result', 'no results', 'no results found',
              'no matching records', 'null', 'none'
          )
      )
  )

{{ config(materialized='table') }}

select
    record_id as website_record_id,
    record_key,
    dataset,
    record_type,
    title,
    category,
    report_type,
    company_name,
    symbol,
    coalesce(published_at::date, report_date) as event_date,
    report_period,
    report_date,
    uploaded_date,
    detail_url,
    download_url,
    source_url,
    raw_bucket,
    raw_key,
    payload,
    first_seen_at,
    last_seen_at
from {{ ref('stg_pse_website_records') }}
where record_type in ('market_report', 'announcement', 'press_release', 'listing_statistic', 'rss_item')

{{ config(materialized='table') }}

select
    record_id as listing_statistic_id,
    record_key,
    statistic_name,
    year,
    main_value,
    sme_value,
    total_value,
    source_url,
    raw_bucket,
    raw_key,
    first_seen_at,
    last_seen_at
from {{ ref('stg_pse_website_records') }}
where dataset = 'listing_statistics'
  and record_type = 'listing_statistic'
  and year is not null

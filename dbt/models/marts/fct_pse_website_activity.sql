select
    dataset,
    coalesce(record_type, 'unknown') as record_type,
    count(*) as record_count,
    max(last_seen_at) as latest_seen_at
from {{ ref('stg_pse_website_records') }}
group by dataset, coalesce(record_type, 'unknown')

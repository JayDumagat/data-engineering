select
    record_id,
    dataset,
    record_key,
    source_url,
    raw_bucket,
    raw_key,
    payload,
    payload ->> 'record_type' as record_type,
    coalesce(payload ->> 'title', payload ->> 'name') as title,
    first_seen_at,
    last_seen_at
from {{ source('pse_website', 'records') }}

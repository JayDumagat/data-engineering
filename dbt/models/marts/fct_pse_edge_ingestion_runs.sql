select
    run_id,
    scope,
    status,
    started_at,
    completed_at,
    raw_object_count,
    stage_record_count,
    curated_record_count,
    failures
from {{ source('pse_edge', 'ingestion_runs') }}

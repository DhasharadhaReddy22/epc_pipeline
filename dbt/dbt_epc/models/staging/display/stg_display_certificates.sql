{{ config(
    materialized='incremental',
    unique_key='lmk_key',
    cluster_by=['load_month'],
    tags=['staging'],
    on_schema_change='sync_all_columns'
) }}

with delta as (

    select 
        r.*,
        a.audit_ts
    from {{ source('raw', 'raw_display_certificates') }} r
    join {{ source('raw', 'raw_copy_audit') }} a 
      on r.audit_id = a.audit_id
)

select
    del.*,
    date_trunc('month', del.audit_ts) as load_month -- for clustering, truncates to first day of month
from delta del

-- Generic incremental filter macro
{{ incremental_filter('del.audit_ts', 'audit_ts') }}
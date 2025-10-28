{{ config(
    materialized='incremental',
    cluster_by=['load_month'],
    tags=['staging']
) }}

with raw_with_audit as (
    select 
        r.*,
        a.audit_ts
    from {{ source('raw', 'raw_domestic_recommendations') }} r
    join {{ source('raw', 'raw_copy_audit') }} a 
      on r.audit_id = a.audit_id
)

select
    r.*,
    to_char(audit_ts, 'YYYY_MM') as load_month
from raw_with_audit r
{{ get_incremental_filter('r.audit_ts') }}
{{ config(
    materialized='incremental',
    unique_key='lmk_key',
    cluster_by=['load_month'],
    tags=['staging']
) }}

with raw_with_audit as (
    select 
        r.*,
        a.audit_ts
    from {{ source('raw', 'raw_non_domestic_certificates') }} r
    join {{ source('raw', 'raw_copy_audit') }} a 
      on r.audit_id = a.audit_id
)

select
    ra.*,
    to_char(ra.audit_ts, 'YYYY-MM') as load_month
from raw_with_audit ra
{{ get_incremental_filter('ra.audit_ts') }}
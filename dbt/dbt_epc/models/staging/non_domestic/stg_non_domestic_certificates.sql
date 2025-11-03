{{ config(
    materialized='incremental',
    unique_key='lmk_key',
    cluster_by=['audit_ts'],
    tags=['staging'],
    on_schema_change='sync_all_columns'
) }}

with delta as (

    select 
        r.*,
        a.audit_ts
    from {{ source('raw', 'raw_non_domestic_certificates') }} r
    join {{ source('raw', 'raw_copy_audit') }} a 
      on r.audit_id = a.audit_id
    {{ incremental_filter('audit_ts') }}
)

select *
from delta
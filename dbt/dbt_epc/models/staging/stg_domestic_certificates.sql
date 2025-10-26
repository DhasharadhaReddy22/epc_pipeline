with source as (
    select 
        r.*,
        a.audit_ts as raw_load_ts
    from {{ ref('raw_domestic_certificates') }} r
    join {{ ref('raw_copy_audit') }} a
      on r.audit_id = a.audit_id
),

incremental as (
    select *
    from source
    {% if is_incremental() %}
      where a.audit_ts > (select max(audit_ts) from {{ this }})
    {% endif %}
)

select * from incremental
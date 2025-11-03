{{ config(
    materialized='incremental',
    unique_key='audit_ts',
    tags=['kpi'],
    schema='presentation'
) }}

with energy as (
    select
        audit_ts,
        avg(current_operational_rating) as monthly_avg_operational_rating,
        round(
            100.0 * sum(case when operational_rating_band in ('A','B','C') then 1 else 0 end) / count(*),
            2
        ) as pct_high_band,
        round(sum(electric_co2 + heating_co2) / nullif(sum(total_floor_area), 0), 2) as monthly_avg_co2_per_m2
    from {{ ref('fact_disp_energy') }}
    
    {% if is_incremental() %}
        where audit_ts > (
            select coalesce(max(audit_ts), to_date('1970-01-01'))
            from {{ this }}
        )
    {% endif %}
    
    group by audit_ts
),

global_summary as (
    select
        round(avg(current_operational_rating), 2) as global_avg_operational_rating,
        round(
            100.0 * sum(case when operational_rating_band in ('A','B','C') then 1 else 0 end) / count(*),
            2
        ) as global_pct_high_band,
        round(sum(electric_co2 + heating_co2) / nullif(sum(total_floor_area), 0), 2) as global_avg_co2_per_m2
    from {{ ref('fact_disp_energy') }}
)

select
    e.audit_ts,
    e.monthly_avg_operational_rating,
    e.pct_high_band,
    e.monthly_avg_co2_per_m2,
    g.global_avg_operational_rating,
    g.global_pct_high_band,
    g.global_avg_co2_per_m2
from energy e
cross join global_summary g
order by e.audit_ts
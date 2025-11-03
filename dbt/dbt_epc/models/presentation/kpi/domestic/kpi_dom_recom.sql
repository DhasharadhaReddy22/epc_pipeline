{{ config(
    materialized='incremental',
    unique_key='audit_ts',
    tags=['kpi']
) }}

with rec_monthly as (
    select
        audit_ts,

        -- Average number of recommendations per property in the month
        round(count(distinct improvement_item)::float / count(distinct lmk_key), 2)
            as monthly_avg_recommendations_per_property,

        -- Total properties and total recommendations
        count(distinct lmk_key) as total_properties,
        count(*) as total_recommendations,

        -- Cost range distribution (based on indicative_cost text patterns)
        round(100.0 * sum(case when lower(indicative_cost) like '%low%' then 1 else 0 end) / count(*), 2)
            as pct_low_cost_range,
        round(100.0 * sum(case when lower(indicative_cost) like '%medium%' then 1 else 0 end) / count(*), 2)
            as pct_medium_cost_range,
        round(100.0 * sum(case when lower(indicative_cost) like '%high%' then 1 else 0 end) / count(*), 2)
            as pct_high_cost_range

    from {{ ref('fact_dom_recom') }}

    {{ incremental_filter('audit_ts') }}

    group by audit_ts
),

global_summary as (
    select
        round(count(distinct improvement_item)::float / count(distinct lmk_key), 2)
            as global_avg_recommendations_per_property,
        count(distinct lmk_key) as global_total_properties,
        count(*) as global_total_recommendations,

        round(100.0 * sum(case when lower(indicative_cost) like '%low%' then 1 else 0 end) / count(*), 2)
            as global_pct_low_cost_range,
        round(100.0 * sum(case when lower(indicative_cost) like '%medium%' then 1 else 0 end) / count(*), 2)
            as global_pct_medium_cost_range,
        round(100.0 * sum(case when lower(indicative_cost) like '%high%' then 1 else 0 end) / count(*), 2)
            as global_pct_high_cost_range

    from {{ ref('fact_dom_recom') }}
)

select
    m.audit_ts,
    m.monthly_avg_recommendations_per_property,
    m.total_properties,
    m.total_recommendations,
    m.pct_low_cost_range,
    m.pct_medium_cost_range,
    m.pct_high_cost_range,
    g.global_avg_recommendations_per_property,
    g.global_total_properties,
    g.global_total_recommendations,
    g.global_pct_low_cost_range,
    g.global_pct_medium_cost_range,
    g.global_pct_high_cost_range
from rec_monthly m
cross join global_summary g
order by m.audit_ts
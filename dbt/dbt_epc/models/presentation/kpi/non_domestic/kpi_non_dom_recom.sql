{{ config(
    materialized='incremental',
    unique_key=['audit_ts', 'payback_type'],
    tags=['kpi']
) }}

with rec_monthly as (
    select
        audit_ts,
        'ALL' as payback_type,  -- overall KPI
        round(count(distinct recommendation_item)::float / count(distinct lmk_key), 2) as monthly_avg_recommendations_per_property,
        count(distinct lmk_key) as total_properties,
        count(*) as total_recommendations
    from {{ ref('fact_non_dom_recom') }}
    
    {{ incremental_filter('audit_ts') }}
    
    group by audit_ts
),

payback_split as (
    select
        audit_ts,
        payback_type,
        round(count(distinct recommendation_item)::float / count(distinct lmk_key), 2) as monthly_avg_recommendations_per_property,
        count(distinct lmk_key) as total_properties,
        count(*) as total_recommendations
    from {{ ref('fact_non_dom_recom') }}
    
    {{ incremental_filter('audit_ts') }}
    
    group by audit_ts, payback_type
),

global_summary as (
    select
        round(count(distinct recommendation_item)::float / count(distinct lmk_key), 2) as global_avg_recommendations_per_property,
        count(distinct lmk_key) as global_total_properties,
        count(*) as global_total_recommendations
    from {{ ref('fact_non_dom_recom') }}
),

combined as (
    select * from rec_monthly
    union all
    select * from payback_split
)

select
    c.audit_ts,
    c.payback_type,
    c.monthly_avg_recommendations_per_property,
    c.total_properties,
    c.total_recommendations,
    g.global_avg_recommendations_per_property,
    g.global_total_properties,
    g.global_total_recommendations
from combined c
cross join global_summary g
order by c.audit_ts, c.payback_type
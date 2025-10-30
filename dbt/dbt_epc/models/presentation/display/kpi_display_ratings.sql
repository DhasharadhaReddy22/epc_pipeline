{{ config(materialized='table', tags=['presentation']) }}

with energy as (
    select
        load_month,
        avg(current_operational_rating) as cur_avg_operational_rating,
        round(100.0 * sum(case when operational_rating_band in ('A','B','C') then 1 else 0 end) / count(*), 2) as pct_high_band,
        round(sum(electric_co2 + heating_co2) / nullif(sum(total_floor_area),0), 2) as cur_avg_co2_per_m2
    from {{ ref('fact_display_energy') }}
    group by load_month
),

rec as (
    select
        load_month,
        round(count(distinct recommendation_item)::float / count(distinct lmk_key), 2) as avg_recommendations_per_property
    from {{ ref('fact_display_recommendations') }}
    group by load_month
)

select
    e.load_month,
    e.cur_avg_operational_rating,
    e.pct_high_band,
    e.cur_avg_co2_per_m2,
    r.avg_recommendations_per_property
from energy e
left join rec r using (load_month)
order by e.load_month
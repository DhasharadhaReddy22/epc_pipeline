{{ config(materialized='table', tags=['presentation']) }}

with rec as (
    select
        r.lmk_key,
        c.load_month,
        r.payback_type,
        count(distinct r.recommendation_item) as rec_count
    from {{ ref('fact_display_recommendations') }} r
    join {{ ref('dim_display_property') }} c
        on r.lmk_key = c.lmk_key
            and r.load_month = c.valid_from
    where c.current_flag = true
    group by r.lmk_key, r.payback_type, c.load_month -- number of recommendations per property per payback_type on load_month
)

select
    load_month,
    payback_type,
    count(distinct lmk_key) as total_properties,
    sum(rec_count) as total_recommendations,
    round(avg(rec_count), 2) as avg_rec_per_property
from rec
group by load_month, payback_type
order by load_month, payback_type
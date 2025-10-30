{{ config(
    materialized='incremental',
    unique_key=['lmk_key', 'load_month'],
    tags=['presentation'],
    cluster_by=['load_month']
) }}

select
    r.lmk_key,
    r.recommendation_item,
    r.recommendation_code,
    r.recommendation,
    r.payback_type,
    r.co2_impact,
    c.load_month
from {{ ref('stg_display_recommendations') }} r
join {{ ref('stg_display_certificates') }} c 
    using (lmk_key)

{% if is_incremental() %}
    where c.load_month > (select coalesce(max(load_month), to_timestamp_ltz('1971-01-01 00:00:00.000 +0000')) from {{ this }})
{% endif %}
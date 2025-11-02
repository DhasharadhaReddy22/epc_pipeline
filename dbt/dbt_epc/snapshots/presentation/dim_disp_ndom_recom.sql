{% snapshot dim_disp_ndom_recom %}

{{ config(
    target_schema='presentation',
    strategy='check',
    unique_key=['payback_type', 'recommendation_code', 'recommendation'],
    check_cols=['payback_type', 'recommendation_code', 'recommendation']
) }}

select distinct
    lower(trim(payback_type)) as payback_type,
    lower(trim(recommendation_code)) as recommendation_code,
    lower(trim(recommendation)) as recommendation
from (
    select payback_type, recommendation_code, recommendation
    from {{ ref('stg_non_domestic_recommendations') }}
    union all
    select payback_type, recommendation_code, recommendation
    from {{ ref('stg_display_recommendations') }}
) src
where payback_type is not null or recommendation_code is not null or recommendation is not null;

{% endsnapshot %}
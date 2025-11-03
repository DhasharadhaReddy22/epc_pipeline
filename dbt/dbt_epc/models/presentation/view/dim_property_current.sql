{{ config(
    materialized='view', 
    tags=['view']
) }}

select
    *,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('dim_property') }}
where dbt_valid_to is null
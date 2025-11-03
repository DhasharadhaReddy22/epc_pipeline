{{ config(
    materialized='incremental',
    unique_key=['lmk_key', 'recommendation_code', 'audit_ts'],
    tags=['fact'],
    cluster_by=['audit_ts']
) }}

with base as (
    select
        *
    from {{ ref('stg_non_domestic_recommendations') }}
    {{ incremental_filter('audit_ts') }}
)

select * from base
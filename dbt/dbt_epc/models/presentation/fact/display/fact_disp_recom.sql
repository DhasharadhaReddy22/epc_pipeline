{{ config(
    materialized='incremental',
    unique_key=['lmk_key', 'recommendation_code', 'audit_ts'],
    tags=['fact'],
    schema='presentation',
    cluster_by=['audit_ts']
) }}

with base as (
    select
        *
    from {{ ref('stg_display_recommendations') }}
    {{ incremental_filter('audit_ts') }}
)

select * from base
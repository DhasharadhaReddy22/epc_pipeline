{{ config(
    materialized='incremental',
    unique_key='audit_ts',
    tags=['kpi']
) }}

with energy as (
    select
        audit_ts,

        -- Energy performance metrics
        round(avg(asset_rating), 2) as monthly_avg_asset_rating,

        -- % of properties rated A–C (energy-efficient buildings)
        round(100.0 * sum(case when asset_rating_band in ('A+', 'A', 'B', 'C') then 1 else 0 end) / count(*), 2) as pct_high_band,

        -- CO₂ and emission intensity
        round(avg(building_emissions), 2) as monthly_avg_emissions_per_m2,
        round(sum(building_emissions * floor_area) / nullif(sum(floor_area), 0), 2) as monthly_weighted_emissions_per_m2,

        -- Benchmark comparison (average gap between actual vs target/standard)
        round(avg(building_emissions - target_emissions), 2) as avg_gap_vs_target,
        round(avg(building_emissions - standard_emissions), 2) as avg_gap_vs_standard,

        -- Primary energy value intensity
        round(avg(primary_energy_value), 2) as monthly_avg_primary_energy_value,

        -- Air conditioning and environment presence
        round(100.0 * sum(case when lower(aircon_present) = 'yes' then 1 else 0 end) / count(*), 2) as pct_with_aircon,
        count(distinct building_environment) as distinct_building_environments

    from {{ ref('fact_non_dom_energy') }}
    
    {{ incremental_filter('audit_ts') }}

    group by audit_ts
),

global_summary as (
    select
        round(avg(asset_rating), 2) as global_avg_asset_rating,
        round(100.0 * sum(case when asset_rating_band in ('A+', 'A', 'B', 'C') then 1 else 0 end) / count(*), 2) as global_pct_high_band,
        round(avg(building_emissions), 2) as global_avg_emissions_per_m2,
        round(sum(building_emissions * floor_area) / nullif(sum(floor_area), 0), 2) as global_weighted_emissions_per_m2,
        round(avg(building_emissions - target_emissions), 2) as global_avg_gap_vs_target,
        round(avg(building_emissions - standard_emissions), 2) as global_avg_gap_vs_standard,
        round(avg(primary_energy_value), 2) as global_avg_primary_energy_value,
        round(100.0 * sum(case when lower(aircon_present) = 'yes' then 1 else 0 end) / count(*), 2) as global_pct_with_aircon,
        count(distinct building_environment) as global_distinct_building_environments
    from {{ ref('fact_non_dom_energy') }}
)

select
    e.audit_ts,
    e.monthly_avg_asset_rating,
    e.pct_high_band,
    e.monthly_avg_emissions_per_m2,
    e.monthly_weighted_emissions_per_m2,
    e.avg_gap_vs_target,
    e.avg_gap_vs_standard,
    e.monthly_avg_primary_energy_value,
    e.pct_with_aircon,
    e.distinct_building_environments,
    g.global_avg_asset_rating,
    g.global_pct_high_band,
    g.global_avg_emissions_per_m2,
    g.global_weighted_emissions_per_m2,
    g.global_avg_gap_vs_target,
    g.global_avg_gap_vs_standard,
    g.global_avg_primary_energy_value,
    g.global_pct_with_aircon,
    g.global_distinct_building_environments
from energy e
cross join global_summary g
order by e.audit_ts
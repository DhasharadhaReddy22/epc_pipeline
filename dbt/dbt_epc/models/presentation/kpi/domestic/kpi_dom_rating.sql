{{ config(
    materialized='incremental',
    unique_key='audit_ts',
    tags=['kpi']
) }}

with energy as (
    select
        audit_ts,

        -- Average energy efficiency & rating
        round(avg(current_energy_efficiency), 2) as monthly_avg_efficiency,
        round(avg(potential_energy_efficiency), 2) as monthly_potential_efficiency,

        -- Average CO2 emissions (total and per m²)
        round(avg(co2_emissions_current), 2) as monthly_avg_co2_tonnes,
        round(sum(co2_emissions_current) / nullif(sum(total_floor_area), 0), 2) as monthly_avg_co2_per_m2,

        -- Energy consumption intensity (kWh/m²)
        round(sum(energy_consumption_current) / nullif(sum(total_floor_area), 0), 2) as monthly_avg_consumption_per_m2,

        -- Avg total cost (lighting + heating + hot water)
        round(avg(lighting_cost_current + heating_cost_current + hot_water_cost_current), 2) as monthly_avg_energy_cost,

        -- % of properties with mains gas
        round(100.0 * sum(case when lower(mains_gas_flag) = 'y' then 1 else 0 end) / count(*), 2) as pct_with_mains_gas,

        -- % of properties rated A–C
        round(100.0 * sum(case when current_energy_rating in ('A','B','C') then 1 else 0 end) / count(*), 2) as pct_high_rating

    from {{ ref('fact_dom_energy') }}
    
    {{ incremental_filter('audit_ts') }}

    group by audit_ts
),

global_summary as (
    select
        round(avg(current_energy_efficiency), 2) as global_avg_efficiency,
        round(avg(potential_energy_efficiency), 2) as global_potential_efficiency,
        round(avg(co2_emissions_current), 2) as global_avg_co2_tonnes,
        round(sum(co2_emissions_current) / nullif(sum(total_floor_area), 0), 2) as global_avg_co2_per_m2,
        round(sum(energy_consumption_current) / nullif(sum(total_floor_area), 0), 2) as global_avg_consumption_per_m2,
        round(avg(lighting_cost_current + heating_cost_current + hot_water_cost_current), 2) as global_avg_energy_cost,
        round(100.0 * sum(case when lower(mains_gas_flag) = 'y' then 1 else 0 end) / count(*), 2) as global_pct_with_mains_gas,
        round(100.0 * sum(case when current_energy_rating in ('A','B','C') then 1 else 0 end) / count(*), 2) as global_pct_high_rating
    from {{ ref('fact_dom_energy') }}
)

select
    e.audit_ts,
    e.monthly_avg_efficiency,
    e.monthly_potential_efficiency,
    e.monthly_avg_co2_tonnes,
    e.monthly_avg_co2_per_m2,
    e.monthly_avg_consumption_per_m2,
    e.monthly_avg_energy_cost,
    e.pct_with_mains_gas,
    e.pct_high_rating,
    g.global_avg_efficiency,
    g.global_potential_efficiency,
    g.global_avg_co2_tonnes,
    g.global_avg_co2_per_m2,
    g.global_avg_consumption_per_m2,
    g.global_avg_energy_cost,
    g.global_pct_with_mains_gas,
    g.global_pct_high_rating
from energy e
cross join global_summary g
order by e.audit_ts
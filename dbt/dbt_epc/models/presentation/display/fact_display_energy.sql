{{ config(
    materialized='incremental',
    unique_key=['lmk_key', 'load_month'],
    tags=['presentation'],
    cluster_by=['load_month']
) }}

select
    lmk_key, audit_ts, load_month, 
    current_operational_rating, yr1_operational_rating, yr2_operational_rating, 
    aircon_kw_rating, estimated_aircon_kw_rating, 
    electric_co2, yr1_electricity_co2, yr2_electricity_co2, 
    heating_co2, yr1_heating_co2, yr2_heating_co2, 
    renewables_co2, yr1_renewables_co2, yr2_renewables_co2, 
    renewables_electrical, renewables_fuel_thermal, 
    annual_thermal_fuel_usage, annual_electrical_fuel_usage, 
    typical_thermal_fuel_usage, typical_electrical_fuel_usage, special_energy_uses, 
    operational_rating_band, main_benchmark, 
    total_floor_area, lodgement_date, nominated_date, or_assessment_end_date, inspection_date
from {{ ref('stg_display_certificates') }}
{{ get_incremental_filter('audit_ts') }}
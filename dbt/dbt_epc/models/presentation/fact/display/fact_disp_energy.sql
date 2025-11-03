{{ config(
    materialized='incremental',
    unique_key=['lmk_key', 'audit_ts'],
    tags=['fact'],
    cluster_by=['audit_ts']
) }}

with base as (
    select
        lmk_key,
        audit_ts,
        current_operational_rating,
        yr1_operational_rating,
        yr2_operational_rating,
        aircon_kw_rating,
        estimated_aircon_kw_rating,
        electric_co2,
        yr1_electricity_co2,
        yr2_electricity_co2,
        heating_co2,
        yr1_heating_co2,
        yr2_heating_co2,
        renewables_co2,
        yr1_renewables_co2,
        yr2_renewables_co2,
        renewables_electrical,
        renewables_fuel_thermal,
        annual_thermal_fuel_usage,
        annual_electrical_fuel_usage,
        typical_thermal_fuel_usage,
        typical_electrical_fuel_usage,
        special_energy_uses,
        operational_rating_band,
        main_benchmark,
        total_floor_area,
        lodgement_date,
        nominated_date,
        or_assessment_end_date,
        inspection_date,

        coalesce(
            nullif(cast(uprn as varchar), ''),
            md5(concat_ws('|',
                replace(lower(coalesce(trim(address1), 'NA')), ' ', ''),
                replace(lower(coalesce(trim(address2), 'NA')), ' ', ''),
                replace(lower(coalesce(trim(address3), 'NA')), ' ', ''),
                replace(lower(coalesce(trim(postcode), 'NA')), ' ', ''),
                coalesce(building_reference_number, 'NA')
            ))
        ) as property_identity_key

    from {{ ref('stg_display_certificates') }}

    {{ incremental_filter('audit_ts') }}
)

select * from base
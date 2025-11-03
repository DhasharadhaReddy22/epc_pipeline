{{ config(
    materialized='incremental',
    unique_key=['lmk_key', 'audit_ts'],
    tags=['fact'],
    cluster_by=['audit_ts']
) }}

with base as (

    select
        coalesce(
            nullif(cast(uprn as varchar), ''),
            md5(concat_ws('|',
                replace(lower(coalesce(trim(address1), 'NA')), ' ', ''),
                replace(lower(coalesce(trim(address2), 'NA')), ' ', ''),
                replace(lower(coalesce(trim(address3), 'NA')), ' ', ''),
                replace(lower(coalesce(trim(postcode), 'NA')), ' ', ''),
                coalesce(building_reference_number, 'NA')
            ))
        ) as property_identity_key,

        lmk_key,
        audit_ts,
        current_energy_rating,
        potential_energy_rating,
        current_energy_efficiency,
        potential_energy_efficiency,
        environmental_impact_current,
        environmental_impact_potential,
        energy_consumption_current,
        energy_consumption_potential,
        co2_emissions_current,
        co2_emiss_curr_per_floor_area,
        co2_emissions_potential,
        total_floor_area,
        lighting_cost_current,
        lighting_cost_potential,
        heating_cost_current,
        heating_cost_potential,
        hot_water_cost_current,
        hot_water_cost_potential,
        energy_tariff,
        mains_gas_flag,
        floor_level,
        flat_top_storey,
        flat_storey_count,
        main_heating_controls,
        multi_glaze_proportion,
        glazed_type,
        glazed_area,
        extension_count,
        number_habitable_rooms,
        number_heated_rooms,
        low_energy_lighting,
        number_open_fireplaces,
        main_fuel,
        wind_turbine_count,
        solar_water_heating_flag,
        photo_supply,
        mechanical_ventilation,
        transaction_type,
        tenure,
        report_type,
        inspection_date,
        lodgement_date

    from {{ ref('stg_domestic_certificates') }}
    {{ incremental_filter('audit_ts') }}
)

select * from base
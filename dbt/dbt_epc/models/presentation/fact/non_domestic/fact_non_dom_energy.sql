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
        asset_rating,
        asset_rating_band,
        property_type,
        main_heating_fuel,
        other_fuel_desc,
        floor_area,
        standard_emissions,
        target_emissions,
        typical_emissions,
        building_emissions,
        aircon_present,
        aircon_kw_rating,
        estimated_aircon_kw_rating,
        building_environment,
        existing_stock_benchmark,
        new_build_benchmark,
        transaction_type,
        inspection_date,
        lodgement_date,
        primary_energy_value,
        report_type

    from {{ ref('stg_non_domestic_certificates') }}
    {{ incremental_filter('audit_ts') }}
)

select * from base
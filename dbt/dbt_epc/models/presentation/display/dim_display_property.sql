-- working model of scd2 for display property dimension, the incremental_strategy will automatically
-- take care of inserting only new records based on unique_key defined as lmk_key and valid_from
{{ config(
    materialized='incremental',
    unique_key=['lmk_key', 'valid_from'],
    incremental_strategy='merge',
    tags=['presentation'],
    cluster_by=['load_month']
) }}

with src as (
    select
        lmk_key, load_month, uprn, postcode, building_reference_number, property_type,
        building_environment, building_category, local_authority, constituency, county, posttown,
        aircon_present, ac_inspection_commissioned, main_heating_fuel, other_fuel,
        renewable_sources, total_floor_area, audit_ts,
        load_month as valid_from,
        null::date as valid_to,
        true as current_flag
    from {{ ref('stg_display_certificates') }}
)

select * from src

-- The following is the more elaborate version og scd2 merge logic, but for some reason throwing errors in dbt
{# {{ config(
    materialized='incremental',
    unique_key=['lmk_key', 'valid_from'],
    tags=['presentation'],
    cluster_by=['load_month']
) }}

with src as (
    select
        lmk_key, load_month, uprn, postcode, building_reference_number, property_type, building_environment, building_category, 
        local_authority, constituency, county, posttown, aircon_present, ac_inspection_commissioned, main_heating_fuel, other_fuel, 
        renewable_sources, total_floor_area, audit_ts,
        md5(concat_ws('|',
            coalesce(postcode, 'NA'),
            coalesce(property_type, 'NA'),
            coalesce(building_category, 'NA'),
            coalesce(building_environment, 'NA'),
            coalesce(total_floor_area::string, 'NA'),
            coalesce(main_heating_fuel, 'NA'),
            coalesce(aircon_present, 'NA'),
            coalesce(renewable_sources, 'NA')
        )) as record_property_hash
    from {{ ref('stg_display_certificates') }}
)

{% if is_incremental() %}
    -- Expire old records (set current_flag=false and valid_to=new load_month)
    merge into {{ this }} as T
    using src as S
        on T.lmk_key = S.lmk_key and T.current_flag = true

        when matched and T.record_property_hash != S.record_property_hash then
        update set -- invalidate old record
            T.current_flag = false,
            T.valid_to = S.load_month

        -- The actual 'new' record (the changed row) is added because,
        -- after the current becomes current_flag=false, 
        -- there's no record with current_flag=true and the new hash for that lmk_key, 
        -- so the when not matched clause 'fires' and inserts the new changed record.
        
        when not matched then
        insert (
            lmk_key, load_month, uprn, postcode, building_reference_number, property_type, building_environment, building_category, 
            local_authority, constituency, county, posttown, aircon_present, ac_inspection_commissioned, main_heating_fuel, other_fuel, 
            renewable_sources, total_floor_area, record_property_hash, audit_ts, valid_from, valid_to, current_flag
        )
        values (
            S.lmk_key, S.load_month, S.uprn, S.postcode, S.building_reference_number, S.property_type, S.building_environment, S.building_category,
            S.local_authority, S.constituency, S.county, S.posttown, S.aircon_present, S.ac_inspection_commissioned, S.main_heating_fuel, S.other_fuel,
            S.renewable_sources, S.total_floor_area, S.record_property_hash, S.audit_ts, S.load_month, null, true
        );

{% else %}
    select
        *,
        load_month as valid_from,
        null::string as valid_to,
        true as current_flag
    from src
{% endif %} #}
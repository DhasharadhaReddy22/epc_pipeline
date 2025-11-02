{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    cluster_by=['valid_from'],
    tags=['presentation'],
    post_hook="{{ property_scd2_merge(this, ref('property_delta')) }}"
) }}

-- Base select to create the table schema if it doesn't exist
select
    cast(null as varchar) as property_sk,
    cast(null as varchar) as property_identity_key,
    cast(null as varchar) as property_bk,
    cast(null as varchar) as address1,
    cast(null as varchar) as address2,
    cast(null as varchar) as address3,
    cast(null as varchar) as postcode,
    cast(null as varchar) as local_authority,
    cast(null as varchar) as local_authority_label,
    cast(null as varchar) as constituency,
    cast(null as varchar) as constituency_label,
    cast(null as varchar) as county,
    cast(null as varchar) as posttown,
    cast(null as varchar) as property_type,
    cast(null as varchar) as built_form,
    cast(null as varchar) as construction_age_band,
    cast(null as varchar) as tenure,
    cast(null as varchar) as building_reference_number,
    cast(null as varchar) as uprn,
    cast(null as varchar) as report_type,
    cast(null as timestamp_ltz) as valid_from,
    cast(null as timestamp_ltz) as valid_to,
    cast(null as boolean) as is_current,
    cast(null as timestamp_ltz) as audit_ts
where false
{% snapshot dim_property %}

{{ config(
    target_schema='presentation',
    strategy='check',
    unique_key='property_identity_key',
    check_cols=['property_bk'],
    tags=['dim']
) }}

select
    address1, 
    address2, 
    address3, 
    postcode,
    local_authority, 
    local_authority_label,
    constituency,
    constituency_label,
    county,
    posttown,
    property_type,
    built_form,
    construction_age_band,
    tenure,
    building_reference_number,
    uprn,
    report_type,
    audit_ts,
    property_bk,
    property_identity_key
from {{ ref('delta_property') }}

{% endsnapshot %}
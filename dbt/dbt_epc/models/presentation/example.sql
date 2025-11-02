{{ config(
    materialized='table',
    schema='presentation'
)}}

{% set max_dim_ts_query %}
    select coalesce(
        (
            select audit_ts
            from {{ source('raw', 'raw_copy_audit') }}
            order by audit_ts desc
            limit 1 offset 6
        ),
        '1970-01-01'::timestamp
    ) as max_audit_ts
{% endset %}

{% set results = run_query(max_dim_ts_query) %}
{% if execute %}
    {% set max_dim_ts = results.columns[0].values()[0] %}
{% else %}
    {% set max_dim_ts = '1970-01-01' %}
{% endif %}

with dom as (
    select
        lower(trim(address1)) as address1,
        lower(trim(address2)) as address2,
        lower(trim(address3)) as address3,
        lower(trim(postcode)) as postcode,
        lower(trim(local_authority)) as local_authority,
        lower(trim(local_authority_label)) as local_authority_label,
        lower(trim(constituency)) as constituency,
        lower(trim(constituency_label)) as constituency_label,
        lower(trim(county)) as county,
        lower(trim(posttown)) as posttown,
        lower(trim(property_type)) as property_type,
        lower(trim(built_form)) as built_form,
        lower(trim(construction_age_band)) as construction_age_band,
        lower(trim(tenure)) as tenure,
        building_reference_number,
        uprn,
        report_type,
        audit_ts
    from {{ ref('stg_domestic_certificates') }} sdc
    where audit_ts > '{{ max_dim_ts }}'
),

non_dom as (
    select
        lower(trim(address1)) as address1,
        lower(trim(address2)) as address2,
        lower(trim(address3)) as address3,
        lower(trim(postcode)) as postcode,
        lower(trim(local_authority)) as local_authority,
        lower(trim(local_authority_label)) as local_authority_label,
        lower(trim(constituency)) as constituency,
        lower(trim(constituency_label)) as constituency_label,
        lower(trim(county)) as county,
        lower(trim(posttown)) as posttown,
        lower(trim(property_type)) as property_type,
        null as built_form,
        null as construction_age_band,
        null as tenure,
        building_reference_number,
        uprn,
        report_type,
        audit_ts
    from {{ ref('stg_non_domestic_certificates') }} sndc
    where audit_ts > '{{ max_dim_ts }}'
),

disp as (
    select
        lower(trim(address1)) as address1,
        lower(trim(address2)) as address2,
        lower(trim(address3)) as address3,
        lower(trim(postcode)) as postcode,
        lower(trim(local_authority)) as local_authority,
        lower(trim(local_authority_label)) as local_authority_label,
        lower(trim(constituency)) as constituency,
        lower(trim(constituency_label)) as constituency_label,
        lower(trim(county)) as county,
        lower(trim(posttown)) as posttown,
        lower(trim(property_type)) as property_type,
        null as built_form,
        null as construction_age_band,
        null as tenure,
        building_reference_number,
        uprn,
        report_type,
        audit_ts
    from {{ ref('stg_display_certificates') }} sdc
    where audit_ts > '{{ max_dim_ts }}'
),

combined as (
    select * from dom
    union all
    select * from non_dom
    union all
    select * from disp
),
deduped as (
    select
        *,
        md5(concat_ws('|',
            coalesce(lower(trim(address1)), 'NA'),
            coalesce(lower(trim(postcode)), 'NA')
        )) as property_identity_key,
        md5(concat_ws('|',
            coalesce(lower(trim(address1)), 'NA'),
            coalesce(lower(trim(postcode)), 'NA'),
            coalesce(lower(trim(local_authority)), 'NA'),
            coalesce(lower(trim(property_type)), 'NA'),
            coalesce(lower(trim(built_form)), 'NA'),
            coalesce(lower(trim(construction_age_band)), 'NA'),
            coalesce(lower(trim(tenure)), 'NA')
        )) as property_bk,
        row_number() over (
            partition by md5(concat_ws('|',
                coalesce(lower(trim(address1)), 'NA'),
                coalesce(lower(trim(postcode)), 'NA')
            ))
            order by audit_ts desc
        ) as rn
    from combined
)
select * from deduped where rn = 1
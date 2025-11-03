{{ config(
    materialized='table',
    schema='presentation',
    tags=['delta']
)}}

-- Determine whether dim_property exists (to detect first run)
{% set dim_relation = adapter.get_relation(
    database=target.database,
    schema='PRESENTATION',
    identifier='DIM_PROPERTY'
) %}

{% if dim_relation %}
    {% set max_dim_ts_query %}
        select coalesce(max(audit_ts), '1970-01-01'::timestamp) as max_audit_ts
        from presentation.dim_property
    {% endset %}
{% else %}
    {% set max_dim_ts_query %}
        select '1970-01-01'::timestamp as max_audit_ts
    {% endset %}
{% endif %}

{% set results = run_query(max_dim_ts_query) %}
{% if execute %}
    {% set max_dim_ts = results.columns[0].values()[0] %}
{% else %}
    {% set max_dim_ts = '1970-01-01' %}
{% endif %}

with dom as (
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
        audit_ts
    from {{ ref('stg_domestic_certificates') }} sdc
    where audit_ts > '{{ max_dim_ts }}'
),

non_dom as (
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

        md5(concat_ws('|',
            replace(lower(coalesce(trim(local_authority), 'NA')), ' ', ''),
            replace(lower(coalesce(trim(local_authority_label), 'NA')), ' ', ''),
            replace(lower(coalesce(trim(constituency), 'NA')), ' ', ''),
            replace(lower(coalesce(trim(constituency_label), 'NA')), ' ', ''),
            replace(lower(coalesce(trim(county), 'NA')), ' ', ''),
            replace(lower(coalesce(trim(posttown), 'NA')), ' ', ''),
            replace(lower(coalesce(trim(property_type), 'NA')), ' ', ''),
            replace(lower(coalesce(trim(built_form), 'NA')), ' ', ''),
            replace(lower(coalesce(trim(construction_age_band), 'NA')), ' ', ''),
            replace(lower(coalesce(trim(tenure), 'NA')), ' ', '')
        )) as property_bk,

        row_number() over (
            partition by coalesce(
                nullif(cast(uprn as varchar), ''),
                md5(concat_ws('|',
                    replace(lower(coalesce(trim(address1), 'NA')), ' ', ''),
                    replace(lower(coalesce(trim(address2), 'NA')), ' ', ''),
                    replace(lower(coalesce(trim(address3), 'NA')), ' ', ''),
                    replace(lower(coalesce(trim(postcode), 'NA')), ' ', ''),
                    coalesce(building_reference_number, 'NA')
                ))
            )
            order by audit_ts desc
        ) as rn
    from combined
)
select * from deduped where rn = 1
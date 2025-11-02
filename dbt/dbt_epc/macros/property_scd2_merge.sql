-- macros/property_merge.sql
{% macro property_scd2_merge(this, delta) %}

    {% set merge_sql %}
        merge into {{ this }} as T
        using ( {{ delta }} ) as D
        on T.property_identity_key = D.property_identity_key and T.is_current = true

        when matched and T.property_bk != D.property_bk then
            update set
                T.is_current = false,
                T.valid_to = current_timestamp()

        when not matched then
            insert (
                property_sk, property_identity_key, property_bk,
                address1, address2, address3, postcode, local_authority, local_authority_label,
                constituency, constituency_label, county, posttown, property_type,
                built_form, construction_age_band, tenure,
                building_reference_number, uprn, report_type,
                valid_from, valid_to, is_current, audit_ts
            )
            values (
                uuid_string(), D.property_identity_key, D.property_bk,
                D.address1, D.address2, D.address3, D.postcode, D.local_authority, D.local_authority_label,
                D.constituency, D.constituency_label, D.county, D.posttown, D.property_type,
                D.built_form, D.construction_age_band, D.tenure,
                D.building_reference_number, D.uprn, D.report_type,
                current_timestamp(), null, true, D.audit_ts
            );
    {% endset %}

    {% do run_query(merge_sql) %}
{% endmacro %}
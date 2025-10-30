{% macro merge_scd2(target, source_query) %}
    {% set sql %}
        merge into {{ target }} as T
        using ({{ source_query }}) as S
        on T.lmk_key = S.lmk_key and T.current_flag = true

        when matched and T.record_property_hash != S.record_property_hash then
            update set
                current_flag = false,
                valid_to = S.load_month

        when not matched then
            insert (
                lmk_key, load_month, uprn, postcode, building_reference_number, property_type,
                building_environment, building_category, local_authority, constituency, county, posttown,
                aircon_present, ac_inspection_commissioned, main_heating_fuel, other_fuel,
                renewable_sources, total_floor_area, record_property_hash, valid_from, valid_to, current_flag
            )
            values (
                S.lmk_key, S.load_month, S.uprn, S.postcode, S.building_reference_number, S.property_type,
                S.building_environment, S.building_category, S.local_authority, S.constituency, S.county,
                S.posttown, S.aircon_present, S.ac_inspection_commissioned, S.main_heating_fuel, S.other_fuel,
                S.renewable_sources, S.total_floor_area, S.record_property_hash, S.load_month, null, true
            );
    {% endset %}

    {% do run_query(sql) %}
{% endmacro %}
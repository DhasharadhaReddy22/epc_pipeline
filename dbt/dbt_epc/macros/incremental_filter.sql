{% macro incremental_filter(source_timestamp_col, target_timestamp_col=None, default_date="1970-01-01") %}
  {#
    - source_timestamp_col: timestamp column in the source table.
    - target_timestamp_col: column in the target model to compare (defaults to same as source).
    - default_date: fallback date if table is empty.
  #}

  {% if target_timestamp_col is none %}
    {% set target_timestamp_col = source_timestamp_col %}
  {% endif %}

  {% if is_incremental() %}
    {% set max_query %}
        select coalesce(
            max({{ target_timestamp_col }}),
            to_timestamp_ltz('{{ default_date }}')
        ) as max_ts
        from {{ this }}
    {% endset %}
    {% set results = run_query(max_query) %}
    {% if execute %}
        {% set max_ts = results.columns[0].values()[0] %}
    {% else %}
        {% set max_ts = default_date %}
    {% endif %}

    where {{ source_timestamp_col }} > to_timestamp_ltz('{{ max_ts }}')

  {% endif %}
{% endmacro %}
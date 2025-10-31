{% macro incremental_filter(source_timestamp_col, target_timestamp_col=None, default_date="1970-01-01") %}
  {# 
    Generic incremental filter that can be reused across layers.
    - source_timestamp_col: source timestamp column (e.g., audit_ts, updated_at, last_modified)
    - target_timestamp_col: optional target timestamp to compare with; defaults to same as source.
    - default_date: fallback timestamp if table is empty.
  #}

  {% if is_incremental() %}
    where {{ source_timestamp_col }} > (
      select coalesce(
        max({{ target_timestamp_col }}),
        to_timestamp_ltz('{{ default_date }}')
      )
      from {{ this }} {# this refers to the current/target model #}
    )
  {% endif %}
{% endmacro %}
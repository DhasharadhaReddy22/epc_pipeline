{% macro get_incremental_filter(timestamp_col, default_date="1970-01-01") %}
  {% if is_incremental() %}
    where {{ timestamp_col }} > (
      select coalesce(
        max(audit_ts),
        to_timestamp_ltz('{{ default_date }}')
      )
      from {{ this }}
    )
  {% endif %}
{% endmacro %}
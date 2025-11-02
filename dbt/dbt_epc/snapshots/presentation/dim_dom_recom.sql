{% snapshot dim_dom_recom %}

{{ config(
    target_schema='presentation',
    strategy='check',
    unique_key=['improvement_id', 'improvement_id_text', 'improvement_descr_text'],
    check_cols=['improvement_id', 'improvement_id_text', 'improvement_descr_text']
) }}

select distinct
    lower(trim(improvement_id)) as improvement_id,
    lower(trim(improvement_id_text)) as improvement_id_text,
    lower(trim(improvement_descr_text)) as improvement_descr_text,
    lower(trim(improvement_summary_text)) as improvement_summary_text,
    lower(trim(indicative_cost)) as indicative_cost
from {{ ref('stg_domestic_recommendations') }}
where improvement_id is not null;

{% endsnapshot %}
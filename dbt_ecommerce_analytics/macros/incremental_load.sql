{% macro incremental_load(unique_key, updated_at_col) %}

  {% if is_incremental() %}

    where {{ updated_at_col }} > (select max({{ updated_at_col }}) from {{ this }})

  {% endif %}

{% endmacro %}
{% macro incremental_load(unique_key, updated_at_col) %}

  {% if is_incremental() %}

    {{ log("Incremental filter applied on " ~ updated_at_col, info=True) }}

    where {{ updated_at_col }} > (select max({{ updated_at_col }}) from {{ this }})

  {% endif %}

{% endmacro %}
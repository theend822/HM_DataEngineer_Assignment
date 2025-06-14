{% macro latest_ds(table_name) %}
  (SELECT MAX(ds) FROM {{ ref(table_name) }})
{% endmacro %}
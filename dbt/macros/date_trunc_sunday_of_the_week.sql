-- This macro truncates a date to the start of the week (Sunday)

{% macro date_trunc_week_sunday(date_column) %}
  (DATE_TRUNC('week', {{ date_column }}) - INTERVAL '1 day')::DATE
{% endmacro %}
-- This macro truncates a date to the start of the week (Sunday)

{% macro date_trunc_sunday_of_the_week(date_column) %}
  (DATE_TRUNC('week', {{ date_column }}) - INTERVAL '1 day')::DATE
{% endmacro %}
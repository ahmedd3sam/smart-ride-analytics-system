{% macro calculate_trip_duration(pickup_col, dropoff_col) %}
    DATEDIFF('minute', {{ pickup_col }}, {{ dropoff_col }})
{% endmacro %}

{% macro is_weekend(date_col) %}
case
when extract(dow from {{ date_col }}) in (6, 0) then true
else false
end
{% endmacro %}
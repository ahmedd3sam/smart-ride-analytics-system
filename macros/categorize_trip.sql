{% macro categorize_trip(duration) %}
case
when {{ duration }} < 10 then 'short'
when {{ duration }} between 10 and 20 then 'medium'
else 'long'
end
{% endmacro %}
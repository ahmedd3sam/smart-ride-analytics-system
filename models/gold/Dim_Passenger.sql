select
row_number() over (order by passenger_count) as passenger_id,
passenger_count
from (
select distinct passenger_count from {{ ref('stg_taxi_trips') }}
) t
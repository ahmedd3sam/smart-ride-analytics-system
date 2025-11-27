select 
    d.year,
    d.month,
    count(f.trip_id) as total_trips,
    avg(f.trip_distance) as avg_distance,
    avg(f.trip_duration) as avg_trip_duration_seconds
from {{ ref('Fact_Taxi_Trips') }} f
join {{ ref('Dim_DateTime') }} d on f.pickUp_time_id = d.dateTime_id
group by 1,2
order by 1,2

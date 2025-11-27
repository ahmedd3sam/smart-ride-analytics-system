select 
    d.year,
    d.month,
    d.day,
    sum(p.total_amount) as daily_revenue,
    sum(p.tip_amount) as daily_tips,
    count(f.trip_id) as total_trips
from {{ ref('Fact_Taxi_Trips') }} f
join {{ ref('Dim_DateTime') }} d on f.pickUp_time_id = d.dateTime_id
join {{ ref('Dim_Payment') }} p on f.payment_id = p.payment_id
group by 1,2,3
order by 1,2,3

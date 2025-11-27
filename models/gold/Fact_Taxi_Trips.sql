with src as (SELECT * FROM {{ ref( 'stg_taxi_trips') }}),


pick_dt as (
select dateTime_id,full_datetime  from {{ ref('Dim_DateTime') }}
),


locs as (
select zone_id from {{ ref('Dim_Location') }}
),


passengers as (
select passenger_id, passenger_count from {{ ref('Dim_Passenger') }}
),


payments as (
select payment_id, fare_amount, total_amount from {{ ref('Dim_Payment') }}
)


select
md5(concat(coalesce(to_varchar(s.vendor_id),'')||'|', coalesce(to_varchar(s.pickup_datetime),'')||'|', coalesce(to_varchar(s.dropoff_datetime),''))) as trip_id,
s.vendor_id,
pd.dateTime_id as pickUp_time_id,
dd.dateTime_id as dropOff_time_id,
zl.zone_id as pickUp_location_id,
zl.zone_id as dropOff_location_id,
p.passenger_id,
pay.payment_id,
s.rate_code,
s.trip_distance,
datediff('second', s.pickup_datetime, s.dropoff_datetime) as trip_duration
from src s
left join pick_dt pd on pd.full_datetime = s.pickup_datetime
left join pick_dt dd on dd.full_datetime = s.dropoff_datetime
left join locs zl on zl.zone_id = s.pickUp_location_id
left join locs dl on dl.zone_id = s.dropOff_location_id
-- left join locs pl on pl.longitude = s.pickup_longitude and pl.latitude = s.pickup_latitude
-- left join locs dl on dl.longitude = s.dropoff_longitude and dl.latitude = s.dropoff_latitude
left join passengers p on p.passenger_count = s.passenger_count
left join payments pay on pay.fare_amount = s.fare_amount and pay.total_amount = s.total_amount
{{ config(
    materialized = 'table'
) }}

WITH trips AS (
    SELECT
        t.trip_id,
        t.vendor_id,
        {{ calculate_trip_duration('dt1.full_datetime', 'dt2.full_datetime') }} AS trip_duration_minutes,
        t.trip_distance,
        {{ safe_divide('t.trip_distance', calculate_trip_duration('dt1.full_datetime', 'dt2.full_datetime')) }} 
            AS distance_per_minute
    FROM {{ ref('Fact_Taxi_Trips') }} t
    left join {{ ref('Dim_DateTime') }} as dt1 on t.pickUp_time_id = dt1.dateTime_id
    left join {{ ref('Dim_DateTime') }} as dt2 on t.dropOff_time_id = dt2.dateTime_id
)

SELECT * FROM trips

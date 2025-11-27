{{ config(materialized='table') }}

WITH base AS (
    SELECT
        d.hour,
        COUNT(*) AS total_trips,
        SUM(p.total_amount) AS total_revenue,
        SUM({{ calculate_trip_duration('d.full_datetime', 'd2.full_datetime') }}) AS total_trip_minutes
    FROM {{ ref('Fact_Taxi_Trips') }} f
    left JOIN {{ ref('Dim_DateTime') }} d ON f.pickUp_time_id = d.datetime_id
    left JOIN {{ ref('Dim_DateTime') }} d2 ON f.dropOff_time_id = d2.datetime_id
    JOIN {{ ref('Dim_Payment') }} p
        ON f.payment_id = p.payment_id
    GROUP BY d.hour
)

SELECT
    *,
    {{ safe_divide('total_revenue', 'total_trips') }} AS avg_revenue_per_trip,
    {{ safe_divide('total_trip_minutes', 'total_trips') }} AS avg_trip_duration
FROM base

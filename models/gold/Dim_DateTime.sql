WITH datetimes AS (
    
    -- pickup datetime
    SELECT pickup_datetime AS dt
    FROM {{ ref('stg_taxi_trips') }}
    WHERE pickup_datetime IS NOT NULL

    UNION

    -- dropoff datetime
    SELECT dropoff_datetime AS dt
    FROM {{ ref('stg_taxi_trips') }}
    WHERE dropoff_datetime IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY dt) AS datetime_id,
    DATE_PART('year', dt) AS year,
    DATE_PART('month', dt) AS month,
    DATE_PART('day', dt) AS day,
    DATE_PART('dow', dt) AS day_of_week,
    TO_CHAR(dt, 'HH24:MI:SS') AS time,
    DATE_PART('hour', dt) AS hour,
    DATE_PART('minute', dt) AS minutes,
    DATE_PART('second', dt) AS seconds,
    dt AS full_datetime
FROM (
    SELECT DISTINCT dt
    FROM datetimes
) t
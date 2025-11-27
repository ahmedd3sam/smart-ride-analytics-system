
select
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    imp_surcharge,
    total_amount,
    payment_type,
    rate_code,
    pickup_location_id,
    dropoff_location_id
from {{ source('RAW', 'NYC_TAXI_TRIPS') }}







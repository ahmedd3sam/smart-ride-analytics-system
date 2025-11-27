-- models/core/dim_payment.sql
select
row_number() over (order by fare_amount, total_amount) as payment_id,
fare_amount,
extra,
mta_tax,
tip_amount,
tolls_amount,
imp_surcharge,
payment_type,
total_amount
from (
select distinct
fare_amount, extra, mta_tax, tip_amount, tolls_amount, imp_surcharge, payment_type, total_amount
from {{ ref('stg_taxi_trips') }}
) t
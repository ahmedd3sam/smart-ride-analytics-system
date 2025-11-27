{% snapshot dim_location_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='zone_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    zone_id,
    zone_name,
    borough,
    zone_geom,
    current_timestamp() as updated_at
from {{ ref('Dim_Location') }}

{% endsnapshot %}

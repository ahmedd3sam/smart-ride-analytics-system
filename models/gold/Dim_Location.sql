SELECT zone_id, zone_name, borough,zone_geom
from {{ref( 'stg_zone_geo')}}
-- models/staging/stg_taxi_zone_lookup.sql
{{ config(materialized='view') }}

select
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
from {{ source('nyc_taxi', 'taxi_zone_lookup_in') }}


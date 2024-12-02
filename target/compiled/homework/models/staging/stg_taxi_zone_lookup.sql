-- models/staging/stg_taxi_zone_lookup.sql


select
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
from `dataengine_learning`.`default`.`taxi_zone_lookup_in`
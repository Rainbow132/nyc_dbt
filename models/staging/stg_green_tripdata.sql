-- models/staging/stg_green_tripdata.sql
{{ config(materialized='view') }}

select
    -- 标识符
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    cast(vendorid as bigint) as vendor_id,
    'Green' as service_type,
    
    -- 时间戳
    cast(lpep_pickup_datetime as datetime) as pickup_datetime,
    cast(lpep_dropoff_datetime as datetime) as dropoff_datetime,
    
    -- trip info
    cast(store_and_fwd_flag as string) as store_and_fwd_flag,
    cast(ratecodeid as bigint) as ratecode_id,
    cast(passenger_count as bigint) as passenger_count,
    cast(trip_distance as double) as trip_distance,
    
    -- location info
    cast(pulocationid as bigint) as pickup_locationid,
    cast(dolocationid as bigint) as dropoff_locationid,
    
    -- payment info
    cast(payment_type as bigint) as payment_type,
    cast(fare_amount as double) as fare_amount,
    cast(extra as double) as extra,
    cast(mta_tax as double) as mta_tax,
    cast(tip_amount as double) as tip_amount,
    cast(tolls_amount as double) as tolls_amount,
    cast(improvement_surcharge as double) as improvement_surcharge,
    cast(total_amount as double) as total_amount,
    cast(congestion_surcharge as double) as congestion_surcharge
    
from {{ source('nyc_taxi', 'green_taxi_trips_in') }}
where extract(year from lpep_pickup_datetime) in (2019, 2020)

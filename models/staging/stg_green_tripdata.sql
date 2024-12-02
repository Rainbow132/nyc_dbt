-- models/staging/stg_green_tripdata.sql
{{ config(materialized='view') }}

select
    -- 标识符
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendor_id,
    'Green' as service_type,
    
    -- 时间戳
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- 行程信息
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(ratecodeid as integer) as ratecode_id,
    
    -- 位置信息
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- 支付信息
    cast(payment_type as integer) as payment_type,
    cast(fare_amount as numeric) as fare_amount,
    cast(total_amount as numeric) as total_amount

from {{ source('nyc_taxi', 'green_taxi_trips_in') }}
where extract(year from lpep_pickup_datetime) in (2019, 2020)

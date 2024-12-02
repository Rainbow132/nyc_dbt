{{ config(materialized='view') }}

select 
    -- 标识符，这里generate_surrogate_key()是一个自定义的宏，通过对几个参数的哈希生成一个唯一的标识符，保证了数据的唯一性，因为这里数据集的vendorid可以是重复的
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendorid as bigint) as vendor_id,
    'Yellow' as service_type,
    
    -- timestamps
    cast(tpep_pickup_datetime as datetime) as pickup_datetime,
    cast(tpep_dropoff_datetime as datetime) as dropoff_datetime,
    
    -- trip info
    cast(passenger_count as bigint) as passenger_count,
    cast(trip_distance as double) as trip_distance,
    cast(ratecodeid as bigint) as ratecode_id,
    
    -- location info
    cast(pulocationid as bigint) as pickup_locationid,
    cast(dolocationid as bigint) as dropoff_locationid,
    
    -- payment info
    cast(payment_type as bigint) as payment_type,
    cast(fare_amount as double) as fare_amount,
    cast(total_amount as double) as total_amount
from {{ source('nyc_taxi', 'yellow_taxi_trips_in') }}
where extract(year from tpep_pickup_datetime) in (2019, 2020)

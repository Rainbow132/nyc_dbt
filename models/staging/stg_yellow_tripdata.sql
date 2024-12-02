{{ config(materialized='view') }}

select 
    -- 标识符，这里{{ dbt_utils.generate_surrogate_key() }}是一个自定义的宏，通过对几个参数的哈希生成一个唯一的标识符，保证了数据的唯一性，因为这里数据集的vendorid可以是重复的
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendor_id,
    'Yellow' as service_type,

    -- 时间戳
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
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
from {{ source('nyc_taxi', 'yellow_taxi_trips_in') }}
where extract(year from tpep_pickup_datetime) in (2019, 2020)

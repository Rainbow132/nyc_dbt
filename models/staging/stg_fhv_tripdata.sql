-- models/staging/stg_fhv_tripdata.sql
{{ config(materialized='view') }}

select
    -- 标识符
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    'FHV' as service_type,
    
    -- 时间戳
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- 位置信息
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- FHV特有信息
    cast(sr_flag as integer) as sr_flag,
    affiliated_base_number

from {{ source('nyc_taxi', 'fhv_taxi_trips_in') }}
where extract(year from pickup_datetime) = 2019

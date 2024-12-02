-- models/staging/stg_fhv_tripdata.sql
{{ config(materialized='view') }}

select
    -- 标识符
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(affiliated_base_number as string) as affiliated_base_number,
    'FHV' as service_type,
    
    -- 时间戳
    cast(pickup_datetime as datetime) as pickup_datetime,
    cast(dropoff_datetime as datetime) as dropoff_datetime,
    
    -- 位置信息
    cast(pulocationid as BIGINT) as pickup_locationid,
    cast(dolocationid as BIGINT) as dropoff_locationid,
    
    -- FHV特有信息
    cast(sr_flag as BIGINT) as sr_flag

from {{ source('nyc_taxi', 'fhv_taxi_trips_in') }}
where extract(year from pickup_datetime)  in (2019, 2020)

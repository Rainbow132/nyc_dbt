CREATE OR REPLACE VIEW `dataengine_learning`.`default`.`stg_fhv_tripdata__dbt_tmp` AS (-- models/staging/stg_fhv_tripdata.sql


select
    -- 标识符
    case when concat(coalesce(cast(dispatching_base_num as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(pickup_datetime as string), '_dbt_utils_surrogate_key_null_')) = NULL
        then md5('')
    else
        md5(concat(coalesce(cast(dispatching_base_num as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(pickup_datetime as string), '_dbt_utils_surrogate_key_null_')))
    end as tripid,
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

from `dataengine_learning`.`default`.`fhv_taxi_trips_in`
where extract(year from pickup_datetime)  in (2019, 2020));

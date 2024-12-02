

-- 1. 首先处理绿色出租车数据，ref 函数用于引用其他模型，并管理模型之间的依赖关系。
--这里的 WITH ... AS 结构是 SQL 中的公用表表达式（Common Table Expression，简称 CTE），它允许在一个查询中定义临时结果集，这些结果集可以在后续的查询中引用。
with green_trips as (
    select 
        tripid,
        vendor_id,
        'Green' as service_type,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        ratecode_id,
        pickup_locationid,
        dropoff_locationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        congestion_surcharge
    from `dataengine_learning`.`default`.`stg_green_tripdata`
),

-- 2. 处理黄色出租车数据
yellow_trips as (
    select 
        tripid,
        vendor_id,
        'Yellow' as service_type,
        pickup_datetime,
        dropoff_datetime,
        null as store_and_fwd_flag,
        ratecode_id,
        pickup_locationid,
        dropoff_locationid,
        passenger_count,
        trip_distance,
        fare_amount,
        null as extra,
        null as mta_tax,
        null as tip_amount,
        null as tolls_amount,
        null as improvement_surcharge,
        total_amount,
        payment_type,
        null as congestion_surcharge
    from `dataengine_learning`.`default`.`stg_yellow_tripdata`
),

-- 3. 合并两种数据，union all 是 SQL 中的集合运算符，用于合并两个或多个查询的结果集，union all 会保留重复的行，而 union 会去除重复的行。
trips_unioned as (
    select * from green_trips
    union all
    select * from yellow_trips
),

-- 4. 关联维度信息，INNER JOIN 用于连接两个表，而 ON 子句用于指定连接条件。
final as (
    select 
        trips_unioned.tripid,
        trips_unioned.vendor_id,
        trips_unioned.service_type,
        trips_unioned.pickup_datetime,
        trips_unioned.dropoff_datetime,
        trips_unioned.store_and_fwd_flag,
        trips_unioned.ratecode_id,
        trips_unioned.pickup_locationid,
        trips_unioned.dropoff_locationid,
        trips_unioned.passenger_count,
        trips_unioned.trip_distance,
        trips_unioned.fare_amount,
        trips_unioned.extra,
        trips_unioned.mta_tax,
        trips_unioned.tip_amount,
        trips_unioned.tolls_amount,
        trips_unioned.improvement_surcharge,
        trips_unioned.total_amount,
        trips_unioned.payment_type,
        trips_unioned.congestion_surcharge,
        pickup_zone.borough as pickup_borough,
        pickup_zone.zone as pickup_zone,
        dropoff_zone.borough as dropoff_borough,
        dropoff_zone.zone as dropoff_zone

    from trips_unioned
    inner join `dataengine_learning`.`default`.`dim_zones` as pickup_zone
        on trips_unioned.pickup_locationid = pickup_zone.locationid
    inner join `dataengine_learning`.`default`.`dim_zones` as dropoff_zone
        on trips_unioned.dropoff_locationid = dropoff_zone.locationid
)

select * from final
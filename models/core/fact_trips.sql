{{ config(materialized='table') }}

-- 1. 首先处理绿色出租车数据，ref 函数用于引用其他模型，并管理模型之间的依赖关系。
--这里的 WITH ... AS 结构是 SQL 中的公用表表达式（Common Table Expression，简称 CTE），它允许在一个查询中定义临时结果集，这些结果集可以在后续的查询中引用。
with green_trips as (
    select 
        *,
        'Green' as service_type 
    from {{ ref('stg_green_tripdata') }}
),

-- 2. 处理黄色出租车数据
yellow_trips as (
    select 
        *,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
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
        trips_unioned.*,
        pickup_zone.borough as pickup_borough,
        pickup_zone.zone as pickup_zone,
        dropoff_zone.borough as dropoff_borough,
        dropoff_zone.zone as dropoff_zone
    from trips_unioned
    inner join {{ ref('dim_zones') }} as pickup_zone
        on trips_unioned.pickup_locationid = pickup_zone.locationid
    inner join {{ ref('dim_zones') }} as dropoff_zone
        on trips_unioned.dropoff_locationid = dropoff_zone.locationid
)

select * from final

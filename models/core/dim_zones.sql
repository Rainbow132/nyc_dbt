-- models/core/dim_zones.sql 维度表示例
{{ config(materialized='table') }}

select 
    locationid,          -- 维度主键
    borough,            -- 描述性属性：行政区
    zone,              -- 描述性属性：具体区域
    service_zone       -- 描述性属性：服务区域类型
from {{ ref('taxi_zone_lookup') }}
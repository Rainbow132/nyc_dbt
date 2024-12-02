
  
    CREATE TABLE IF NOT EXISTS `dataengine_learning`.`default`.`dim_zones__dbt_tmp`
    
    AS (
      -- models/core/dim_zones.sql 维度表示例


select 
    locationid,          -- 维度主键
    borough,            -- 描述性属性：行政区
    zone,              -- 描述性属性：具体区域
    service_zone       -- 描述性属性：服务区域类型
from `dataengine_learning`.`default`.`stg_taxi_zone_lookup`
    )
    ;

  
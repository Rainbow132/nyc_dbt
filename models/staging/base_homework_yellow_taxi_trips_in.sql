with source as (
      select * from {{ source('homework', 'yellow_taxi_trips_in') }}
),
renamed as (
    select
        {{ adapter.quote("vendorid") }},
        {{ adapter.quote("tpep_pickup_datetime") }},
        {{ adapter.quote("tpep_dropoff_datetime") }},
        {{ adapter.quote("passenger_count") }},
        {{ adapter.quote("trip_distance") }},
        {{ adapter.quote("ratecodeid") }},
        {{ adapter.quote("store_and_fwd_flag") }},
        {{ adapter.quote("pulocationid") }},
        {{ adapter.quote("dolocationid") }},
        {{ adapter.quote("payment_type") }},
        {{ adapter.quote("fare_amount") }},
        {{ adapter.quote("extra") }},
        {{ adapter.quote("mta_tax") }},
        {{ adapter.quote("tip_amount") }},
        {{ adapter.quote("tolls_amount") }},
        {{ adapter.quote("improvement_surcharge") }},
        {{ adapter.quote("total_amount") }},
        {{ adapter.quote("congestion_surcharge") }}

    from source
)
select * from renamed
  
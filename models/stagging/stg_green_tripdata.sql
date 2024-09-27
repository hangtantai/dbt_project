{{ config(materialized="view") }}

select 
    {{ dbt_utils.surrogate_key(['vendor_id', 'pickup_datetime']) }} as tripid,
    cast(vendor_id as integer) as vendor_id,
    cast(rate_code as numeric)  as rate_code,
    cast(pickup_location_id as integer) as  pickup_location_id,
    cast(dropoff_location_id as integer) as dropoff_location_id,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(imp_surcharge as numeric) as imp_surcharge,
    cast(total_amount as numeric) as total_amount,
    coalesce(cast(cast(payment_type as numeric) as integer), 0) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast(airport_fee as numeric) as airport_fee
from `my-second-project-434008`.`trips_data_all`.`green_tripdata`
where vendor_id is not null

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
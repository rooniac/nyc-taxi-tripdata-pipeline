{{
  config(
    materialized = 'incremental',
    unique_key='trip_id'
    )
}}

WITH day_part_mapping AS (
    SELECT
        label, 
        day_part_id
    FROM {{ ref('dim_day_part') }}
)

SELECT 
    ROW_NUMBER() OVER () AS trip_id,
    stg.vendorid  AS vendor_key,
    stg.ratecodeid AS rate_code_key,
    stg.payment_type AS payment_type_key,
    stg.service_type AS service_type_id,
    stg.pulocationid AS pickup_location_id,
    stg.dolocationid AS dropoff_location_id,
    dpm.day_part_id,
    stg.pickup_datetime,
    stg.dropoff_datetime,
    stg.trip_distance,
    stg.total_amount,
    stg.passenger_count,
    stg.fare_amount,
    stg.tip_amount,
    stg.tolls_amount,
    stg.mta_tax,
    stg.extra,
    stg.improvement_surcharge,
    stg.congestion_surcharge,
    stg.updated_at
FROM {{ source('staging', 'stg_tripdata') }} AS stg
LEFT JOIN day_part_mapping AS dpm
    ON stg.day_part_id = dpm.label
WHERE stg.trip_key IS NOT NULL

{% if is_incremental() %}
  and stg.updated_at > (SELECT COALESCE(MAX(updated_at), '2023-01-01') FROM {{ this }})
{% endif %}
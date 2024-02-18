{{ 
    config(materialized='view')
}}

with dim_zones as (
    select * from {{ ref('dim_zones')}} where borough != 'Unknown'
),
fhv_tripdata as(
    select 
    *,
    'Fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
)

SELECT
    fhv_data.tripid,
	fhv_data.vendorid,
    fhv_data.service_type,
    fhv_data.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    fhv_data.affiliated_base_number,
    fhv_data.sr_flag
FROM
	fhv_tripdata as fhv_data
	INNER JOIN dim_zones AS pickup_zone 
        ON (pickup_zone.locationid = fhv_data.pickup_locationid)
	INNER JOIN dim_zones AS dropoff_zone 
        ON (dropoff_zone.locationid = fhv_data.dropoff_locationid)
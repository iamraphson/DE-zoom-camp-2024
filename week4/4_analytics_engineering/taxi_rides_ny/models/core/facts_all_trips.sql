{{ 
    config(materialized='table')
}}

with green_tripdata as (
    select *, 
    'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),
yellow_tripdata as (
    select *, 
    'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),
fhv_tripdata as (
    select *,
    'Fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),
trips_unioned AS (
	SELECT
		tripid,
		vendorid,
		service_type,
		pickup_locationid,
		dropoff_locationid,
		pickup_datetime,
		dropoff_datetime
	FROM
		green_tripdata
	UNION ALL
	SELECT
		tripid,
		vendorid,
		service_type,
		pickup_locationid,
		dropoff_locationid,
		pickup_datetime,
		dropoff_datetime
	FROM
		yellow_tripdata
	UNION ALL
	SELECT
		tripid,
		vendorid,
		service_type,
		pickup_locationid,
		dropoff_locationid,
		pickup_datetime,
		dropoff_datetime
	FROM
		fhv_tripdata
),
dim_zones as (
    select * from {{ ref('dim_zones')}} 
    where borough != 'Unknown' 
)

SELECT
    trips.tripid,
	trips.vendorid,
    trips.service_type,
    trips.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    trips.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    trips.pickup_datetime,
    trips.dropoff_datetime,
FROM
	trips_unioned as trips
	INNER JOIN dim_zones AS pickup_zone 
        ON (pickup_zone.locationid = trips.pickup_locationid)
	INNER JOIN dim_zones AS dropoff_zone 
        ON (dropoff_zone.locationid = trips.dropoff_locationid)
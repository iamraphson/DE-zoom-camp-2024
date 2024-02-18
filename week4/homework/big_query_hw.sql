CREATE OR REPLACE EXTERNAL TABLE
  radiant-gateway-412001.all_trips_data.external_fhv_2019_tripdata OPTIONS( 
    format='PARQUET',
    uris = ["gs://dezoomcamp_2024_storage_bucket_radiant-gateway-412001/fhv_2019_parquet/fhv_tripdata_2019-*.parquet"]
);


CREATE OR REPLACE TABLE
  `radiant-gateway-412001.all_trips_data.fhv_2019_tripdata` AS 
SELECT
  *
FROM
  `radiant-gateway-412001.all_trips_data.external_fhv_2019_tripdata`;


SELECT
  count(*), SR_Flag
FROM
 radiant-gateway-412001.homework_trips_data.fhv_2019_tripdata group by SR_Flag;

 SELECT
  count(*), dispatching_base_num
FROM
 radiant-gateway-412001.homework_trips_data.fhv_2019_tripdata group by dispatching_base_num;

  SELECT
  count(*), Affiliated_base_number
FROM
 radiant-gateway-412001.homework_trips_data.fhv_2019_tripdata group by Affiliated_base_number;

SELECT
  count(*), VendorID
FROM
 radiant-gateway-412001.trips_data_all.green_tripdata group by VendorID;
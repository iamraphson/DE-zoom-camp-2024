create or replace external table radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata
OPTIONS (
  format = 'CSV',
  uris = ["gs://dezoomcamp_2024_storage_bucket_radiant-gateway-412001/green_2019_csv/green_tripdata_2019-*.csv.gz", "gs://dezoomcamp_2024_storage_bucket_radiant-gateway-412001/green_2019_csv/green_tripdata_2020-*.csv.gz"]
);

select * from radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata limit 5;


-- Create non partition table
CREATE OR REPLACE TABLE radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata_non_partitoned AS
SELECT * FROM radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata;

-- Create partition table
CREATE OR REPLACE TABLE radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata_partitoned 
PARTITION BY DATE(lpep_pickup_datetime)
AS SELECT * FROM radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata;

-- Scanning 89.07MB of Data
select distinct(VendorID) from radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata_non_partitoned
where DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' and '2019-06-30';

-- Scanning 7.16MB of Data
select distinct(VendorID) from radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata_partitoned
where DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' and '2019-06-30';

--- More datails about the partitions
select table_name, partition_id, total_rows
from radiant-gateway-412001.nytaxi.INFORMATION_SCHEMA.PARTITIONS
where table_name = 'external_green_2019_2020_tripdata_partitoned' order by total_rows DESC;

--- More datails about the partitions and clusters
select table_name, partition_id, total_rows
from radiant-gateway-412001.nytaxi.INFORMATION_SCHEMA.PARTITIONS
where table_name = 'external_green_2019_2020_tripdata_partitoned_clustered' order by total_rows DESC;

-- Create partition and cluster table
CREATE OR REPLACE TABLE radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime) cluster by VendorID
AS SELECT * FROM radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata;


select count(*) from radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata_partitoned
where DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' and '2019-12-31' and VendorID=1;


select count(*) from radiant-gateway-412001.nytaxi.external_green_2019_2020_tripdata_partitoned_clustered
where DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' and '2019-12-31' and VendorID=1;





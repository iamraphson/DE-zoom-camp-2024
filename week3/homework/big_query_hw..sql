-- Create external table
create or replace external table radiant-gateway-412001.week3_homework.external_green_2022_tripdata
options(
  format='PARQUET',
  uris = ["gs://dezoomcamp_2024_storage_bucket_radiant-gateway-412001/green_2022_parquet/green_tripdata_2022-*.parquet"]
)

-- Create non partition table
create or replace table radiant-gateway-412001.week3_homework.external_green_2022_tripdata_non_partitoned as
select * from radiant-gateway-412001.week3_homework.external_green_2022_tripdata;

-- Create partition table
create or replace table radiant-gateway-412001.week3_homework.external_green_2022_tripdata_partitoned
partition by date(lpep_pickup_datetime) as 
select * from radiant-gateway-412001.week3_homework.external_green_2022_tripdata;

-- Create partition and cluster table
create or replace table radiant-gateway-412001.week3_homework.external_green_2022_tripdata_partitoned_clustered
partition by date(lpep_pickup_datetime) cluster by PUlocationID as 
select * from radiant-gateway-412001.week3_homework.external_green_2022_tripdata;


select count(distinct(PULocationID)) from radiant-gateway-412001.week3_homework.external_green_2022_tripdata;

select count(distinct(PULocationID)) from radiant-gateway-412001.week3_homework.external_green_2022_tripdata_non_partitoned;

-- count records that have a fare_amount of 0
select count(*) from radiant-gateway-412001.week3_homework.external_green_2022_tripdata_non_partitoned where fare_amount = 0;

--use_cache=false
select distinct(PULocationID) from radiant-gateway-412001.week3_homework.external_green_2022_tripdata_non_partitoned where Date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';

--use_cache=false
select distinct(PULocationID)  from radiant-gateway-412001.week3_homework.external_green_2022_tripdata_partitoned where Date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';

select count(*) from radiant-gateway-412001.week3_homework.external_green_2022_tripdata_non_partitoned;


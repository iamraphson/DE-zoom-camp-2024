# Week 3 homework answer

[Assignment](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/03-data-warehouse/homework.md)

### Question 1: What is count of records for the 2022 Green Taxi Data??
![image](https://github.com/iamraphson/react-paystack/assets/3502724/ef476b5a-8e83-4209-abf4-65290fdda30e)

The answer is `840,402`

### Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
_External Table_
![image](https://github.com/iamraphson/react-paystack/assets/3502724/06a9fc61-b663-4586-83c4-729672a24ad4)

_Materialized Table_
![image](https://github.com/iamraphson/react-paystack/assets/3502724/f5a66a3b-3044-4f9d-abbb-ee816dd59f43)

The answer is `0 MB for the External Table and 6.41MB for the Materialized Table`

### Question 3: How many records have a fare_amount of 0?
I wrote below sql 
```sql
select count(*) from radiant-gateway-412001.week3_homework.external_green_2022_tripdata_non_partitoned where fare_amount = 0;
```

The answer is `1622`

### Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
The answer is `Partition by lpep_pickup_datetime Cluster on PUlocationID`

The query to create a new table with this strategy
```sql
-- Create partition and cluster table
create or replace table radiant-gateway-412001.week3_homework.external_green_2022_tripdata_partitoned_clustered
partition by date(lpep_pickup_datetime) cluster by PUlocationID as 
select * from radiant-gateway-412001.week3_homework.external_green_2022_tripdata;
```

### Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

### Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

I wrote below sql 
```sql
select distinct(PULocationID) from radiant-gateway-412001.week3_homework.external_green_2022_tripdata_non_partitoned where Date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';

select distinct(PULocationID)  from radiant-gateway-412001.week3_homework.external_green_2022_tripdata_partitoned where Date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
```
_non-partitioned table_
![image](https://github.com/iamraphson/react-paystack/assets/3502724/b21233ec-202b-471d-85d0-2b83454b61e4)

_partitioned table_
![image](https://github.com/iamraphson/react-paystack/assets/3502724/b5d22d47-aeb1-45e9-80bf-4f9594f879a1)

The answer is `12.82 MB for non-partitioned table and 1.12 MB for the partitioned table`

### Question 6:Where is the data stored in the External Table you created?
The answer is `GCP Bucket`

### Question 6:It is best practice in Big Query to always cluster your data.

The answer is `false`

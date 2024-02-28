# Week 4 homework answer

[Assignment](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/04-analytics-engineering/homework.md)

### Question 1: What happens when we execute dbt build --vars '{'is_test_run':'true'}'?

Answer is `It applies a limit 100 only to our staging models`

### Question 2: What is the code that our CI job will run?

The answer is `The code that is behind the object on the dbt_cloud_pr_ schema`

### Question 3: What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?
![image](https://github.com/iamraphson/react-paystack/assets/3502724/a41890bc-7a3b-4c23-8325-d94a78c0e1eb)

The answer is `22,998,722` trips

### Question 4: What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?

![image](https://github.com/iamraphson/react-paystack/assets/3502724/f148186f-aeac-4e9f-8c9e-9a98c3edc3f2)


In July 2019, the followign serivce has below number of trips
- Yellow: 3,248,482
- Green: 415,386
- FHV: 290,680

The answer is `Yellow`

Below is my dashboard from this homework
![image](https://github.com/iamraphson/react-paystack/assets/3502724/4e217d9e-5de9-4cdd-af23-a90a94e35d92)


- [DAG](web_to_gcs.fhv.ipynb) for converting and  uploading FHV 2019 data
- [Query](big_query_hw.sql) to upload it to Bigquery
- Dimensions:
    - [FHV trip data](../4_analytics_engineering/taxi_rides_ny/models/staging/stg_fhv_tripdata.sql) Dimension for FHV trip data
    - [Yellow trip data](../4_analytics_engineering/taxi_rides_ny/models/staging/stg_yellow_tripdata.sql) Dimension for Yellow trip data
    - [Green trip data](../4_analytics_engineering/taxi_rides_ny/models/staging/stg_green_tripdata.sql.sql) Dimension for Green trip data
- Facts
    - [Green and Yellow facts](../4_analytics_engineering/taxi_rides_ny/models/core/facts_trips.sql) Facts for the combination for yellow and green data
    - [FHV fact](../4_analytics_engineering/taxi_rides_ny/models/core/facts_fhv_trips.sql) Fact FHV data
    - [Green, Yellow and FHV facts](../4_analytics_engineering/taxi_rides_ny/models/core/facts_all_trips.sql) Fact for all trips(Green, Yellow and FHV) data

- Dashboards
    - Green / Yellow dashboard
    ![image](https://github.com/iamraphson/react-paystack/assets/3502724/7a3cdf41-86c8-4bb6-b403-6dcf0252657f)
    - FHV Dashboard
    ![image](https://github.com/iamraphson/react-paystack/assets/3502724/4e217d9e-5de9-4cdd-af23-a90a94e35d92)


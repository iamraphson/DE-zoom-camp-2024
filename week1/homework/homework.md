# Week 1 homework answer
## Docker

### Question 1
I ran `docker run --help`
``` bash
--rm #Automatically remove the container when it exits
```
### Question 2
I raun the following commands:
- `docker run -it --entrypoint="bash" python:3.9`. This pull down the container
- `pip list` in the container bash to get the version of wheel package.
``` bash
wheel      0.42.0
```

## Prepare Postgres
pipeline script is [here](homework.ipynb)

### Question 3
Query used 
```sql
SELECT
	count(*)
FROM
	green_taxi_data_2019_09
WHERE
	CAST(lpep_pickup_datetime AS DATE) = '2019-09-18' and CAST(lpep_dropoff_datetime AS DATE) = '2019-09-18'
```
The answer is `15612`

### Question 4
Query used
```sql
SELECT
	trip_distance,
	CAST(lpep_pickup_datetime AS DATE) AS "day"
FROM
	green_taxi_data_2019_09
ORDER BY trip_distance desc;
```
The answer is `2019-09-26`

### Question 5
Query used
```sql
SELECT
	lpu. "Borough",
	sum(total_amount) as total_sum
FROM
	green_taxi_data_2019_09 AS green
	JOIN zones lpu ON (green. "PULocationID" = lpu. "LocationID" and lpu."Borough" <> 'Unknown')
WHERE
	CAST(lpep_pickup_datetime AS DATE) = '2019-09-18'
GROUP BY
	lpu. "Borough" order by total_sum desc;
```
The answer is `"Brooklyn" "Manhattan" "Queens"`

### Question 6
Query used
```sql
SELECT
	green.tip_amount as tip,
	ldo."Zone"
	
FROM
	green_taxi_data_2019_09 AS green
	JOIN zones lpu ON (green. "PULocationID" = lpu. "LocationID")
	JOIN zones ldo on (green."DOLocationID" = ldo."LocationID")
	where lpu."Zone" = 'Astoria' order by tip desc;

```
The answer is `JFK Airport`


### Question 7
The answer is [here](/week1/homework/terraform)

Output
```bash
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the
following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "nyc_trips_data"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "us-west1"
      + max_time_travel_hours      = (known after apply)
      + project                    = "radiant-gateway-412001"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

  # google_storage_bucket.data_lake_bucket will be created
  + resource "google_storage_bucket" "data_lake_bucket" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "US-WEST1"
      + name                        = "dezoomcamp_2024_storage_bucket_radiant-gateway-412001"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + rpo                         = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }
          + condition {
              + age                   = 80
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data_lake_bucket: Creating...
google_storage_bucket.data_lake_bucket: Creation complete after 0s [id=dezoomcamp_2024_storage_bucket_radiant-gateway-412001]
google_bigquery_dataset.dataset: Creation complete after 1s [id=projects/radiant-gateway-412001/datasets/nyc_trips_data]
```
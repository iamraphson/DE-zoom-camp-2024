{{ 
    config(materialized='view')
}}

with fhvtripdata as 
(
  select *,
  from {{ source('staging','fhv_2019_tripdata') }}
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as vendorid,
    {{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOLocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    Affiliated_base_number as affiliated_base_number,
    cast(SR_Flag as numeric) as sr_Flag,
from fhvtripdata
where dispatching_base_num is not null

-- dbt run -m <model.sql> --var 'is_test_run: false'
-- {% if var('is_test_run', default=true) %}

-- limit 100

--{% endif %}
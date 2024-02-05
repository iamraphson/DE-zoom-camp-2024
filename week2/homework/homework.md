# Week 2 homework answer

[Assignment](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/02-workflow-orchestration/homework.md)

### Question 1: Once the dataset is loaded, what's the shape of the data?

I wrote Data loader script in Mage.ai

```python
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    months = ['10', '11', '12']
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'RatecodeID':pd.Int64Dtype(),
        'PULocationID':pd.Int64Dtype(),
        'DOLocationID':pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'fare_amount': float,
        'extra':float,
        'mta_tax':float,
        'tip_amount':float,
        'tolls_amount':float,
        'ehail_fee': float,
        'improvement_surcharge':float,
        'total_amount':float,
        'payment_type': pd.Int64Dtype(),
        'trip_type': float,
        'congestion_surcharge':float
    }

    dfs = []
    for month in months:
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz'
        df = pd.read_csv(url, compression='gzip', sep=",", dtype=taxi_dtypes,parse_dates=parse_dates)
        dfs.append(df)
    
    three_months_df = pd.concat(dfs, ignore_index=True)
    (rows, columns) = three_months_df.shape
    print('Number of rows', rows)
    print('Number of columns', columns)
    
    return three_months_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```
<img width="324" alt="img1" src="https://github.com/iamraphson/react-paystack/assets/3502724/86fd19f5-7d66-4721-9600-bc537017a78e">

The answer is `266 855 rows x 20 columns`

## Question 2: Upon filtering the dataset where the passenger count is greater than 0 and the trip distance is greater than zero, how many rows are left?

I wrote Data tranformer script below in Mage.ai

```python
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    df_question_2 = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    (rows_question_2, columns_question_2) = df_question_2.shape

    print('rows_question_2', rows_question_2)
```

![image](https://github.com/iamraphson/react-paystack/assets/3502724/2fe3111d-0357-4ad0-a571-79d9ebbb9d08)

## Question 3: Which of the following creates a new column lpep_pickup_date by converting lpep_pickup_datetime to a date?

The answer:
```python
data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
``` 

## Question 4: What are the existing values of VendorID in the dataset?

I used panda script below in Mage.ai

```python
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # existing values of vendor_id in the dataframe
    print(data['VendorID'].unique())
    return data
```

The answer is `[2, 1]`

Question 5: How many columns need to be renamed to snake case?

I used panda script below in Mage.ai

```python
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(data.dtypes)
    return data
```
The answer:
![image](https://github.com/iamraphson/react-paystack/assets/3502724/629782ca-1313-4c2f-9065-626ddf4f0605)

## Question 6: Once exported, how many partitions (folders) are present in Google Cloud?
The answer:
![image](https://github.com/iamraphson/react-paystack/assets/3502724/7e83e672-b074-475b-8124-8b2ae48aca67)

## References:

Data loader
```python
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    months = ['10', '11', '12']
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'RatecodeID':pd.Int64Dtype(),
        'PULocationID':pd.Int64Dtype(),
        'DOLocationID':pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'fare_amount': float,
        'extra':float,
        'mta_tax':float,
        'tip_amount':float,
        'tolls_amount':float,
        'ehail_fee': float,
        'improvement_surcharge':float,
        'total_amount':float,
        'payment_type': pd.Int64Dtype(),
        'trip_type': float,
        'congestion_surcharge':float
    }

    dfs = []
    for month in months:
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz'
        df = pd.read_csv(url, compression='gzip', sep=",", dtype=taxi_dtypes,parse_dates=parse_dates)
        dfs.append(df)
    
    three_months_df = pd.concat(dfs, ignore_index=True)
    (rows, columns) = three_months_df.shape
    print('Number of rows', rows)
    print('Number of columns', columns)
    
    return three_months_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```
Data transfromer
```python
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    df_question_2 = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    (rows_question_2, columns_question_2) = df_question_2.shape

    print('rows_question_2', rows_question_2)
    
    df_with_full_trip = data[(data['passenger_count'] != 0) | (data['trip_distance'] != 0)]

    #create a new column with only pickup date
    df_with_full_trip['lpep_pickup_date'] = df_with_full_trip['lpep_pickup_datetime'].dt.date
    
    #rename VendorId column
    df_with_full_trip = df_with_full_trip.rename(columns={"VendorID": "vendor_id"})
    
    #let's get the shape of the current dataframe
    print('dataframe shape now', df_with_full_trip.shape)

    # existing values of vendor_id in the dataframe
    print('exisiting values for vendor_id', list(df_with_full_trip['vendor_id'].unique()))
    return df_with_full_trip


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output, 'Dataframe contains vendor_id'
    assert output is not None, 'The output is undefined'
    assert output[output['passenger_count'] > 0]['passenger_count'].count() > 0, 'passenger_count dataframe is greater than 0'
    assert output[output['trip_distance'] > 0]['trip_distance'].count() > 0, 'trip_distance dataframe is greater than 0'

```

Data exporter for Google cloud storage
```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    table_id = 'radiant-gateway-412001.mage.green_taxi'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )
```

Data exporter for Google big query
```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/dezoomcamp-2024.json'
bucket_name = 'mage_dezoomcamp_2024_storage_bucket_radiant-gateway-412001'
project_id = 'radiant-gateway-412001'

table_name = 'nyc_green_taxi_q4_2020_data'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
```

DAG flow:
![image](https://github.com/iamraphson/react-paystack/assets/3502724/84de53bb-be32-443e-9ae6-0cc9c4712fb2)
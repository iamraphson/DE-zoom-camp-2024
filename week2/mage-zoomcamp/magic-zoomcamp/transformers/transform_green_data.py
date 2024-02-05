import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df_question_2 = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    (rows_question_2, columns_question_2) = df_question_2.shape

    print('rows_question_2', df_question_2.shape)
    
    df_with_full_trip = data[(data['passenger_count'] > 0) | (data['trip_distance'] > 0)]
    print(df_with_full_trip.shape)

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

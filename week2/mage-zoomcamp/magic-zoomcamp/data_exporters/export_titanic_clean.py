from mage_ai.io.file import FileIO
from mage_ai.settings.repo import get_repo_path
from pandas import DataFrame
from deltalake.writer import write_deltalake
from os import path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_file(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to filesystem.

    Docs: https://docs.mage.ai/design/data-loading#example-loading-data-from-a-file
    
    filepath = 'titanic_clean.csv'
    FileIO().export(df, filepath)
    """

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    bucket_name = 'mage_dezoomcamp_2024_storage_bucket_radiant-gateway-412001'
    object_key = 'titanic_clean.csv'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )



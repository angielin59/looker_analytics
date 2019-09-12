from io import StringIO

import boto3
import pandas as pd

from analytics_pipeline.looker_api.data_dictionary import LookerDataDictionary
from analytics_pipeline.looker_api.client import Looker

#TODO: ADD LOGGGING

DEFAULT_COLUMNS = ['label',
                    'description',
                    'view_name',
                    'field_type',
                    'data_type',
                    'hidden',
                    'sql_query',
                    'explores',
                    'name']

DEFAULT_MODELS = ['01_meetup', '03_split_tests']

def write_df_to_csv_on_s3(df, bucket, filename):
    """Constructs a data dictionary containing "columns" that describe the
    fields in "models" using the labels and descriptions in Looker
    Args:
        columns (list[str]): The columns to include in the data dictionary
        models (list[str]): The models to pull fields from
    Returns:
        df: A pandas dataframe.
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())

def get_data_dictionary(base_url, client_id, client_secret, columns=None, models=None):
    """Constructs a data dictionary containing "columns" that describe the
    fields in "models" using the labels and descriptions in Looker
    Args:
        columns (list[str]): The columns to include in the data dictionary
        models (list[str]): The models to pull fields from
    Returns:
        df: A pandas dataframe.
    """
    if not columns:
        columns = DEFAULT_COLUMNS

    if not models:
        columns = DEFAULT_MODELS

    api = Looker(base_url, client_id, client_secret).get_api()
    datadict = LookerDataDictionary(api, relevant_models=models)
    datadict.construct_data_dict()
    df = pd.DataFrame(
            list(datadict.data_dictionary.values()),
                 columns=columns)
    return df

def write_data_dictionary_to_s3(base_url, client_id, client_secret, path, filename, columns=None, models=None):
    data_dict = get_data_dictionary(base_url, client_id, client_secret)
    write_df_to_csv_on_s3(data_dict, path, filename)

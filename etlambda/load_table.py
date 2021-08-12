import boto3
import io
import pandas as pd
from sqlalchemy import create_engine

def read_json(data, dataset=None):
    return pd.read_json(data, orient='records')
    
def read_xlsx(data, dataset=None):
    return pd.read_excel(data, sheet_name=dataset["sheet_index"])

def read_parquet(data, dataset=None):
    return pd.read_parquet(data)    

READ_FUNCTIONS = {
    'json' : read_json,
    'xlsx' : read_xlsx,
    'pqt' : read_parquet
}

def to_pgsql(s3_params, db_params):
    try:
        s3 = boto3.resource('s3')
        obj = s3.Object(s3_params['bucket_name'], s3_params['file_name'])
        engine = create_engine(db_params['uri'], connect_args={'connect_timeout': db_params['timeout']})

        for dataset in s3_params['datasets']:
            with io.BytesIO(obj.get()['Body'].read()) as data:
                read_function = READ_FUNCTIONS[dataset["content_type"]]
                df = read_function(data, dataset)

                df.to_sql(dataset['table'], 
                          engine, 
                          schema=db_params['schema'],
                          if_exists=db_params['if_exists'], 
                          index=False,
                          chunksize=10000)

    except Exception as e:
        raise e

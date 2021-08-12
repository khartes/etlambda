import boto3
from sqlalchemy import create_engine, text as sqlalchemy_text

def execute_sql_script(bucket_name, script_file, db_uri):
    try:
        result = None
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket_name, script_file)
        query = obj.get()['Body'].read().decode('utf-8')    

        engine = create_engine(db_uri, connect_args={'connect_timeout': 45})

        with engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(sqlalchemy_text(query))
            if result.cursor:
                result = result.cursor.fetchall()
            conn.close()

        return result

    except Exception as e:
        raise e

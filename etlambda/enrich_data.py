import boto3
from sqlalchemy import create_engine, text as sqlalchemy_text

def execute_sql_script(db_uri, input_text=None, input_file=None, bucket_name=None):
    try:
        if (input_text and input_file) or (not input_text and not input_file):
            raise ValueError("Parameter error: 'input_text' XOR 'input_file'")
        if input_file and not bucket_name:
            raise ValueError("Parameter error: 'input_file' requires 'bucket_name'")

        engine = create_engine(db_uri, connect_args={'connect_timeout': 45})
        result = None
        script = ''

        if input_file:
            s3 = boto3.resource('s3')
            obj = s3.Object(bucket_name, input_file)
            script = obj.get()['Body'].read().decode('utf-8')
        else:
            script = input_text

        with engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(sqlalchemy_text(script))
            if result.cursor:
                result = result.cursor.fetchall()
            conn.close()

        return result

    except Exception as e:
        raise e

from gdal2pgsql import raster2pgsql
from sqlalchemy import create_engine, text as sqlalchemy_text

def to_pgsql(s3_params, db_params):
    def execute_sql(sql_content):
        with engine.connect().execution_options(autocommit=True) as conn:
                return conn.execute(sqlalchemy_text(sql_content))
    try:
        engine = create_engine(db_params['uri'])

        src_filename = f"/vsis3/{s3_params['bucket_name']}/{s3_params['file_name']}"   

        for layer in s3_params['datasets']:
            raster2pgsql.raster2pgsql(execute_sql, src_filename, layer['table'], db_params['schema'])

    except Exception as e:
        raise e

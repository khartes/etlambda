import os
from gdal2pgsql import ogr2ogr

LOAD_STRATEGY_MAP = {
    'zip' : '/vsizip/',
    'gzip': '/vsigzip/'
}

def to_pgsql(s3_params, db_params):
    try:
        uri = db_params['uri']
        if_exists = ['', '-overwrite'] if db_params['if_exists'] == 'replace' else ['', '-update', '-append']

        if s3_params['force_source_enconding']:
            os.environ['SHAPE_ENCODING'] =  s3_params['force_source_enconding']

        for layer in s3_params['datasets']:
            load_strategy = ''.join([LOAD_STRATEGY_MAP[i] for i in layer['content_type'].split('.') if i in LOAD_STRATEGY_MAP ])
            aditional_path = f"/{layer['aditional_path']}" if layer['aditional_path'] else ''
            
            params = if_exists + ["-f", "PostgreSQL", f'PG:{uri}',
                "-nln", f"{db_params['schema']}.{layer['table']}",
                "-nlt", "PROMOTE_TO_MULTI",
                "--config", "PG_USE_COPY", "YES",
                "-lco", "SPATIAL_INDEX=NONE",
                "-gt", "5000",
                f"{load_strategy}/vsis3/{s3_params['bucket_name']}/{s3_params['file_name']}{aditional_path}"]

            ogr2ogr.main(params)

    except Exception as e:
        raise e


import boto3
import requests
import uuid
import unidecode

class CustomError(Exception): pass

def from_gbq(**kwargs):
    import pandas_gbq
    from io import BytesIO
    
    def extract_to_s3(gbq_params, credentials, source_name, bucket_name, file_name):  
        random_file_path = str(uuid.uuid4()).split('-')[0]
        prefix = f"{random_file_path}_{source_name}/".replace(' ', '_').lower()
        file_name = generate_safe_name(f"{prefix}{file_name}")
        s3_client = boto3.client('s3')
        df = pandas_gbq.read_gbq(gbq_params['sql'], 
                                project_id=gbq_params['project_id'], 
                                credentials=credentials)
        bytes_ = BytesIO()
        df.to_parquet(path=bytes_)
        bytes_.seek(0)
        s3_client.upload_fileobj(bytes_, bucket_name, file_name)

        return file_name  
    
    try:
        gbq_params = kwargs['gbq_params']
        file_name = f"{kwargs['dataset']}.{kwargs['content_type']}"
        s3_params = {}

        s3_params['file_name'] = extract_to_s3( gbq_params, 
                                                kwargs['credentials'], 
                                                kwargs['source_name'],  
                                                kwargs['bucket_name'], 
                                                file_name)
        s3_params["datasets_type"] = "table"
        s3_params['datasets'] = [{
            'aditional_path': '',
            'content_type': 'pqt',
            'table': f"{kwargs['source_name']}_{kwargs['dataset']}".replace(' ', '_').lower()
        }]
        
        return s3_params
     
    except Exception as e:
        print(e)
        raise e

def from_gee(**kwargs):
    import ee
    import time
    from google.oauth2.credentials import Credentials

    TASK_STATUS = [
        'COMPLETED',
        'FAILED',
        'CANCEL_REQUESTED',
        'CANCELLED'
    ]
    CREDENTIALS = Credentials(
        None,
        refresh_token=kwargs['refresh_token'],
        token_uri=ee.oauth.TOKEN_URI,
        client_id=ee.oauth.CLIENT_ID,
        client_secret=ee.oauth.CLIENT_SECRET,
        scopes=ee.oauth.SCOPES)

    try:
        gee_params = kwargs['gee_params']
        file_name = f"{gee_params['file_format']['fileNamePrefix']}.{kwargs['content_type']}"
        gee_params['file_format']['fileNamePrefix'] = "extracao_gee"
        s3_params = {}
        
        bands = gee_params.pop("bands", None)

        ee.Initialize(credentials=CREDENTIALS)

        img = ee.Image(gee_params["asset_id"])
        if bands:
            img = img.select(bands)       

        task = ee.batch.Export.image.toCloudStorage(img, **gee_params["file_format"])
        task.start()
        atual = None
        is_active = True

        while is_active:
            if atual != task.status()['state']:
                atual = task.status()['state']
                print(atual)
            if atual in TASK_STATUS:
                is_active = False
                if atual != 'COMPLETED':
                    raise Exception(f"Task failed")
            time.sleep(3)
            
        http_params = {
                "url": f"{kwargs['base_url']}/{gee_params['file_format']['bucket']}/extracao_gee.tif",
                "timeout": 30,
                "verify": False,
                "query": {}
        }

        s3_params['file_name'] = to_s3(kwargs['source_name'], 
                                                    None,
                                                    None,
                                                    kwargs['bucket_name'],
                                                    http_params,
                                                    None,
                                                    file_name)
        s3_params["datasets_type"] = "raster"
        s3_params['datasets'] = [{
            'aditional_path': '',
            'content_type': '',
            'table': f"{kwargs['source_name']}_{kwargs['dataset']}".replace(' ', '_').lower()
        }]
        
        return s3_params
     
    except Exception as e:
        print(e)
        raise e

def from_http(**kwargs):
    try:
        s3_params = kwargs["s3_params"]
        s3_params['file_name'] = to_s3( kwargs['source_name'], 
                                        None,
                                        None,
                                        kwargs['bucket_name'],
                                        kwargs["http_params"],
                                        None,
                                        kwargs['file_name'])
        s3_params["datasets"][0]["table"] = f"{kwargs['source_name']}_{kwargs['dataset'].replace(' ', '_')}".lower()          
        return s3_params
     
    except Exception as e:
        raise e

def from_incra(**kwargs):
    from bs4 import BeautifulSoup

    def build_datasets(s3_params, source_name, dataset):
        content_type = s3_params['file_name'].split('.')[-1]
        aditional_path = ''
        
        if content_type.lower() == 'zip':
            aditional_path = s3_params['file_name'].split('/')[-1].replace(f".{content_type}", '.shp')  

        return [{
            "aditional_path": aditional_path,
            "content_type": content_type,
            "table": f"{source_name}_{dataset}".replace(' ', '_').lower()
        }]

    def resolve_url(incra_params):
        try:
            page = requests.post(f"{kwargs['base_url']}/{kwargs['form_resource']}", incra_params, verify=False)   
            if page.status_code != 200: raise CustomError(f"Connection error: {page.status_code}") 

            soup = BeautifulSoup(page.content, 'html.parser')
            hrefs = soup.find_all('a', href=True)
            if len(hrefs) == 0: raise CustomError('HTML parser error: href not found')

            shp_file = hrefs[0]['href'].replace('\\', '/')
            url = f"{kwargs['base_url']}/{shp_file}"
            
            return url

        except Exception as e:
            raise e    
    try:
        http_params = { 
            "url": resolve_url(kwargs['incra_params']),
            "timeout" : 120,
            "verify": False,
            "query": {}
        }
        s3_params = {}
            
        s3_params['file_name'] = to_s3(kwargs['source_name'], 
                                                    None,
                                                    None,
                                                    kwargs['bucket_name'],
                                                    http_params,
                                                    None,
                                                    kwargs['file_name'])
        s3_params["datasets_type"] = "vector"
        s3_params["datasets"] = build_datasets( s3_params,
                                                kwargs['source_name'], 
                                                kwargs['dataset'])

        return s3_params
     
    except Exception as e:
        raise e

def from_sicar(**kwargs):
    from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
    from io import BytesIO
    from PIL import Image
    from remotezip import RemoteZip    
    from twocaptcha import TwoCaptcha

    def resolve_captcha(session, twocaptcha_token, captcha_url):
        try:
            data = session.get(captcha_url).content

            img = Image.open(BytesIO(data))
            img.save('/tmp/captcha.png')
            
            solver = TwoCaptcha(twocaptcha_token)
            result = solver.normal('/tmp/captcha.png')            
            
            return result['code']
        except Exception as e:
            raise e

    def validate_layers(bucket_name, file_key, aws_region, layers):
        aws_service='s3'
        aws_host=f'{aws_service}.{aws_region}.amazonaws.com'

        auth = BotoAWSRequestsAuth(
            aws_host=aws_host,
            aws_region=aws_region,
            aws_service=aws_service)

        url = f'https://{aws_host}/{bucket_name}/{file_key}'
        files = []
        try:
            with RemoteZip(url, auth=auth) as ziped_file: 
                files = [i.split('.')[0] for i in ziped_file.namelist()] 
        except:
            print('Arquivo sicar vazio: %s' % file_key)                
        return list(set(files).intersection(set(layers)))

    try:
        sicar_params = kwargs["sicar_params"]
        file_name = f"{sicar_params['geocodigo']}.zip"
        http_params = {
           "verify": False
        }
        if "http_params" in kwargs:
            http_params = kwargs["http_params"]
                    
        s3_params = {}
        
        session = open_session(kwargs['base_url'], http_params['verify'])

        captcha = resolve_captcha(session, kwargs['twocaptcha_token'], kwargs['captcha_url'])

        http_params = { 
            **http_params,
            "url": kwargs['shape_url'],
            "timeout" : 180,
            "query": { 
                "municipio[id]": sicar_params["geocodigo"],
                "email": sicar_params["email"],
                "captcha": captcha
            }
        }

        s3_params['file_name'] = to_s3(kwargs['source_name'], 
                                                    None,
                                                    None,
                                                    kwargs['bucket_name'],
                                                    http_params,
                                                    session,
                                                    file_name)
        s3_params["datasets_type"] = "vector"
        valid_layers = validate_layers(kwargs['bucket_name'], 
                                        s3_params['file_name'], 
                                        kwargs['aws_region'],
                                        sicar_params["layers"])
        s3_params["datasets"] = [{
            "aditional_path": f"{i}.zip/{i}.shp",
            "content_type": "zip.zip", 
            "table": f"sicar_{i.lower()}" }
            for i in valid_layers]

        return s3_params
    
    except Exception as e:
        raise e

def from_sidra(**kwargs):
    def resolve_url(sidra_params):
        classificação = '/'.join([ f'{k}/{v}' for k, v in  sidra_params['classificacao'].items()])
        url = kwargs['base_url']
        url += f"/t/{sidra_params['tabela']}"
        url += f"/{sidra_params['nivel_territorial']}"
        url += f"/{sidra_params['valor_nivel_territorial']}"
        url += f"/h/{sidra_params['cabecalho']}"
        url += f"/p/{sidra_params['periodo']}"
        url += f"/v/{sidra_params['variavel']}"
        url += f"/{classificação}"
        return url   

    try:
        http_params = kwargs["http_params"]
        http_params["url"] = resolve_url(kwargs["sidra_params"])
        s3_params = {}

        s3_params['file_name'] = to_s3(kwargs['source_name'], 
                                                    None, 
                                                    None, 
                                                    kwargs['bucket_name'], 
                                                    http_params,
                                                    None,
                                                    kwargs['file_name'])
        s3_params["datasets_type"] = "table"
        s3_params["datasets"] = [{
            "aditional_path": "",
            "content_type": "json", 
            "table": f"{kwargs['source_name']}_{kwargs['dataset'].replace(' ', '_')}".lower()
        }]
    
        return s3_params
     
    except Exception as e:
        print(e)
        raise e

def from_wfs(**kwargs):
    def build_datasets(s3_params, source_name, dataset):
        content_type = s3_params['file_name'].split('.')[-1]
        aditional_path = ''
        
        if content_type.lower() == 'zip':
            aditional_path = s3_params['file_name'].split('/')[-1].replace(f".{content_type}", '.shp')  

        return [{
            "aditional_path": aditional_path,
            "content_type": content_type,
            "table": f"{source_name}_{dataset}".replace(' ', '_').lower()
        }]

    try:
        s3_params = {}
        s3_params['file_name'] = to_s3( kwargs['source_name'], 
                                        kwargs['dataset'],
                                        kwargs['content_type'],
                                        kwargs["bucket_name"],
                                        kwargs['http_params'],
                                        None,
                                        kwargs.get('file_name', None))
        s3_params['datasets_type'] = 'vector'
        s3_params['datasets'] = build_datasets( s3_params,
                                                kwargs['source_name'], 
                                                kwargs['dataset'])

        return s3_params
    
    except Exception as e:
        raise e

def to_s3(source_name, dataset, content_type, bucket_name, http_params, session=None, file_name=None):
    try:
        url = requests.Request('GET', http_params['url'], params=http_params['query']).prepare().url
        get = session.get if session else requests.get    
        response = get(url, stream=True, verify=http_params['verify'], timeout=http_params['timeout'])
        if response.status_code != 200: raise CustomError(f"Connection error: {response.status_code}") 
        file_name = resolve_filename(response.headers, source_name, dataset, content_type, file_name)
 
        s3_client = boto3.client('s3')
        s3_client.upload_fileobj(response.raw, bucket_name, file_name)

        return file_name  

    except Exception as e:
        raise e

def open_session(url, verify= False):
    session = requests.Session()
    session.get(url,  verify= verify)
    return session

def resolve_filename(headers, source_name, dataset, content_type, file_name=None):
    random_file_path = str(uuid.uuid4()).split('-')[0]

    prefix = generate_safe_name(f"{random_file_path}_{source_name}/")
    if not file_name:
        content_disposition = headers.get('Content-Disposition', None)
        if content_disposition and 'attachment' in content_disposition and 'filename' in content_disposition:
            file_name = content_disposition.split('=')[-1].replace('"', '').replace("'",'')
        else:
            file_name = generate_safe_name(f"{dataset}.{content_type}.{headers['Content-Encoding']}")
    file_name = f"{prefix}{file_name}"
    
    return file_name

def generate_safe_name(name):
    return unidecode.unidecode('{0}'.format(name)).replace(' ', '_').replace('-', '_').lower()

from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import yaml
import os

#import pyspark
#from pyspark.sql import SparkSession
#import pyspark.sql.functions as sf      # sf = spark functions
#import pyspark.sql.types as st          # st = spark types

#SETUP config and FileSensor data dir path
#------------------------------------------------
config = {'project': 'team-week-3', 'dataset': 'tech_stocks_world_events',
'key_path': "/Users/Ruben/Desktop/google_cred/.cred/team_project_3/team-week-3-2f1d10dceea4.json"}

#_default_config_path = './config.yml'
#CONF_PATH = Variable.get('config_file', default_var=_default_config_path)
#config: dict = {}
#with open(CONF_PATH) as open_yaml:
    #config: dict =  yaml.full_load(open_yaml)
    
data_fs = FSHook(conn_id='data_fs')     # get airflow connection for data_fs
DATA_DIR = data_fs.get_path()  


#Initialize spark for ETL to parquet file
#------------------------------------------------
data_dir = DATA_DIR

file_names = ['AAPL','ADBE','AMZN', 'CRM', 'CSCO', 'GOOGL', 'IBM','INTC','META','MSFT','NFLX','NVDA','ORCL','TSLA'] #excluded AAPL to start df
    
old_names = ['Date','Open','High','Low','Close','Adj Close','Volume']
new_names = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
    
rename_dict = {item[0]:item[1] for item in zip(old_names,new_names)}

li = []

for file in file_names:
    idf = pd.read_csv(os.path.join(data_dir,f'{file}.csv'),header=0)
    
    idf = idf.rename(columns=rename_dict)
    
    idf.insert(0,'stock_name', file)
    
    idf.insert(0,'sd_id', idf['stock_name']+idf['date'].astype(str))
    
    li.append(idf)

df = pd.concat(li, axis=0)

df.to_parquet(os.path.join(data_dir,'all_stocks.parquet'))

#load stocks parquet file into BigQuery
#------------------------------------------------

PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']

key_path = config['key_path']

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

#create bigquery client
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

#create dataset_id and table_ids
dataset_id = f"{PROJECT_NAME}.{DATASET_NAME}"
table_id = f"{PROJECT_NAME}.{DATASET_NAME}.stocks"

#rename the parquet file in order to load to BigQuery
#parq = '.snappy.parquet'
#crc = '.crc'
#for file_name in os.listdir(data_dir):
    #source = data_dir + file_name
    #if parq in source and crc not in source:
        #os.rename(os.path.join(data_dir,file_name),os.path.join(data_dir,'stocks.parquet'))
    
#filepath to get loaded to BigQuery
DATA_FILE = os.path.join(data_dir,'all_stocks.parquet')

TABLE_SCHEMA = [
    bigquery.SchemaField('sd_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('stock_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('open', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('high', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('low', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('close', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('adj_close', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('volume', 'INTEGER', mode='NULLABLE'),
    ]

def create_dataset():
    if client.get_dataset(dataset_id) == NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
    else:
        pass

def create_stocks_table():
    job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            create_disposition='CREATE_NEVER',
            write_disposition='WRITE_TRUNCATE',
            ignore_unknown_values=True,
        )
    table = bigquery.Table(table_id, schema=TABLE_SCHEMA)
    table = client.create_table(table, exists_ok=True)

    with open(DATA_FILE, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()

create_dataset()
create_stocks_table()
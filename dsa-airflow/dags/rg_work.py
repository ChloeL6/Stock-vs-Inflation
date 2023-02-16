from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import yaml
import os
from datetime import datetime


#SETUP config and FileSensor data dir path
#------------------------------------------------

_default_config_path = '/opt/airflow/dags/config.yml'
CONF_PATH = Variable.get('config_file', default_var=_default_config_path)
config: dict = {}
with open(CONF_PATH) as open_yaml:
    config: dict =  yaml.full_load(open_yaml)
    
data_fs = FSHook(conn_id='data_fs')     # get airflow connection for data_fs
data_dir = data_fs.get_path()  

#Initialize spark for ETL to parquet file
#------------------------------------------------

def stocks_transform():
    file_names = ['AAPL','ADBE','AMZN', 'CRM', 'CSCO', 'GOOGL', 'IBM','INTC','META','MSFT','NFLX','NVDA','ORCL','TSLA', 'Bitcoin'] #excluded AAPL to start df

    #renaming the columns   
    old_names = ['Date','Open','High','Low','Close','Adj Close','Volume']
    new_names = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
    rename_dict = {item[0]:item[1] for item in zip(old_names,new_names)}

    #empty list to get all dataframes for concat
    li = []

    #extract and transform all of the files 
    for file in file_names:
        idf = pd.read_csv(os.path.join(data_dir,f'{file}.csv'),header=0)
        #rename the columns
        idf = idf.rename(columns=rename_dict)
        #insert column with the stock name
        idf['date'] = pd.to_datetime(idf['date'], format='%Y-%m-%d')

        idf.insert(0,'day', idf['date'].dt.day)
        idf.insert(0,'month', idf['date'].dt.month)
        idf.insert(0,'year', idf['date'].dt.year)
        
        idf.insert(0,'stock_name', file)
        #insert column with composite key
        idf.insert(0,'sd_id', idf['stock_name']+idf['date'].astype(str))

        li.append(idf)

    #consolidate all files into one
    df = pd.concat(li, axis=0)
    #set index to composite key
    df.set_index('sd_id', inplace=True)
    #save consolidated df into parquet file
    df.to_parquet(os.path.join(data_dir,'all_stocks.parquet'))


#load stocks parquet file into BigQuery
#------------------------------------------------
PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']

#create bigquery client
client = bigquery.Client()

#create dataset_id and table_ids
dataset_id = f"{PROJECT_NAME}.{DATASET_NAME}"
stocks_table_id = f"{PROJECT_NAME}.{DATASET_NAME}.stocks"
m2_table_id = f"{PROJECT_NAME}.{DATASET_NAME}.m2_supply"
g_table_id = f"{PROJECT_NAME}.{DATASET_NAME}.gas_prices"

STOCKS_TABLE_SCHEMA = [
    bigquery.SchemaField('sd_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('stock_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('year', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('month', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('day', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('open', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('high', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('low', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('close', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('adj_close', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('volume', 'INTEGER', mode='NULLABLE'),
    ]

M2_TABLE_SCHEMA = [
    bigquery.SchemaField('date_monthly', 'DATE', mode='REQUIRED'),
    bigquery.SchemaField('year', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('month', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('m2_supply', 'FLOAT', mode='NULLABLE'),
    ]

GAS_TABLE_SCHEMA = [
    bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
    bigquery.SchemaField('year', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('month', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('all_grade_prices', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('reg_grade_prices', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('mid_grade_prices', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('prem_grade_prices', 'FLOAT', mode='NULLABLE'),
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
    table = bigquery.Table(stocks_table_id, schema=STOCKS_TABLE_SCHEMA)
    table = client.create_table(table, exists_ok=True)

    with open(os.path.join(data_dir, 'all_stocks.parquet'), "rb") as source_file:
        job = client.load_table_from_file(source_file, stocks_table_id, job_config=job_config)

    job.result()

def m2_transform():
    m2df = pd.read_csv(os.path.join(data_dir,f'FRB_H6.csv'),header=5)
    m2df = m2df[['Time Period', 'M2_N.M']]
    m2df = m2df.rename(columns={'Time Period': 'date_monthly', 'M2_N.M':'m2_supply'})

    m2df['date_monthly'] = pd.to_datetime(m2df['date_monthly'], format='%Y-%m')

    m2df.insert(0,'month', m2df['date_monthly'].dt.month)
    m2df.insert(0,'year', m2df['date_monthly'].dt.year)

    m2df.to_parquet(os.path.join(data_dir,'m2_supply.parquet'))

def create_m2_table():
    job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            create_disposition='CREATE_NEVER',
            write_disposition='WRITE_TRUNCATE',
            ignore_unknown_values=True,
        )
    table = bigquery.Table(m2_table_id, schema=M2_TABLE_SCHEMA)
    table = client.create_table(table, exists_ok=True)

    with open(os.path.join(data_dir, 'm2_supply.parquet'), "rb") as source_file:
        job = client.load_table_from_file(source_file, m2_table_id, job_config=job_config)

    job.result()

def gas_transform():
    gdf = pd.read_csv(os.path.join(data_dir, 'PET_PRI_GND_DCUS_NUS_W.csv'),header=0)
    gdf = gdf[['Date', 'A1', 'R1', 'M1', 'P1']]
    gdf = gdf.rename(columns={'Date': 'date', 'A1':'all_grade_prices', 'R1':'reg_grade_prices', 'M1':'mid_grade_prices', 'P1':'prem_grade_prices'})

    gdf['date'] = pd.to_datetime(gdf['date'], format='%m/%d/%Y')

    gdf.insert(0,'month', gdf['date'].dt.month)
    gdf.insert(0,'year', gdf['date'].dt.year)

    gdf = gdf.groupby(['date','year','month']).agg({'all_grade_prices': 'mean', 'reg_grade_prices': 'mean', 'mid_grade_prices': 'mean', 'prem_grade_prices': 'mean'})
    gdf = gdf.reset_index()

    gdf.to_parquet(os.path.join(data_dir,'gas_prices.parquet'))

def create_gas_table():
    job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            create_disposition='CREATE_NEVER',
            write_disposition='WRITE_TRUNCATE',
            ignore_unknown_values=True,
        )
    table = bigquery.Table(g_table_id, schema=GAS_TABLE_SCHEMA)
    table = client.create_table(table, exists_ok=True)

    with open(os.path.join(data_dir, 'gas_prices.parquet'), "rb") as source_file:
        job = client.load_table_from_file(source_file, g_table_id, job_config=job_config)

    job.result()
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf      # sf = spark functions
import pyspark.sql.types as st          # st = spark types
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound


sparkql = pyspark.sql.SparkSession.builder.master('local').getOrCreate()

import os
data_dir = '../data'

file_names = ['ADBE','AMZN', 'CRM', 'CSCO', 'GOOGL', 'IBM','INTC','META','MSFT','NFLX','NVDA','ORCL','TSLA'] #excluded AAPL to start df

columns = 'stock_name string, date string, open float, high float, low float, close float, adj_close float, volume int' #schema to use

df = sparkql.read.csv(os.path.join(data_dir,'AAPL.csv'), header=True)
df = df.toDF('date', 'open', 'high', 'low', 'close', 'adj_close', 'volume') #rename the columns
df = df.withColumn('stock_name', sf.lit('AAPL')) #add column with stock name

#create composite key
df = sparkql.sql("SELECT CONCAT(stock_name, date) AS sd_id, stock_name, date, open, high, low, close, adj_close, volume FROM key")
df = df.select('sd_id','stock_name', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume')

#create df for each csv, transform it and then consolidate into one dataframe
for csv in file_names:
    idf = sparkql.read.csv(os.path.join(data_dir,csv+'.csv'), header=True)
    idf = idf.toDF('date', 'open', 'high', 'low', 'close', 'adj_close', 'volume') #rename the columns
    idf = idf.withColumn('stock_name', sf.lit(csv)) #add column with stock name

    #create composite key
    idf.createOrReplaceTempView("key") 
    idf = sparkql.sql("SELECT CONCAT(stock_name, date) AS sd_id, stock_name, date, open, high, low, close, adj_close, volume FROM key")
    idf = idf.select('sd_id','stock_name', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume')
    
    #concat to df
    df = df.union(idf)

# Write to parquet file. Used coalesce in order to have one parquet file
df.coalesce(1).write.format("parquet").save(os.path.join(data_dir,'all_tech_stocks.parquet'))





PROJECT_NAME = 'team-week-3'
DATASET_NAME = 'tech_stocks_world_events'

key_path = "/Users/Ruben/Desktop/google_cred/.cred/team_project_3/team-week-3-2f1d10dceea4.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

#create bigquery client
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

#create dataset_id and table_ids
dataset_id = f"{PROJECT_NAME}.{DATASET_NAME}"
table_id = f"{PROJECT_NAME}.{DATASET_NAME}.stocks"

data_dir = '../data'
DATA_FILE = os.path.join(data_dir,'all_tech_stocks.parquet/part-00000-aa854ce3-ba60-4fb4-abe7-f7df420718dc-c000.snappy.parquet')

TABLE_SCHEMA = [
    bigquery.SchemaField('stock_name', 'STRING', mode='REQUIRED'),
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
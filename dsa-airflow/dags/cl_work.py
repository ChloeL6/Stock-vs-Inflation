import os
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import dag,task
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook


import logging
import sys
import os
import yaml
from airflow.models import Variable
import time
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound



# -------------------------------------------
# Set up logging
# -------------------------------------------

# setup logging and logger
logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=logging.INFO,
                    stream=sys.stderr)


# define logger
logger: logging.Logger = logging.getLogger()
logger.setLevel(level=logging.INFO)


def get_this_dir(filepath: str = __file__) -> str:
    """helper function to return this (python) file directory"""
    return os.path.dirname(os.path.abspath(filepath))



# -------------------------------------------
# Load config file
# -------------------------------------------

# default config file path when running locally (not within airflow docker container)
# get the path from airflow variables OR set it to default local path
_default_config_path = os.path.join(get_this_dir(), './config.yml')
CONF_PATH = Variable.get('config_file', default_var=_default_config_path)
config: dict = {}
with open(CONF_PATH) as open_yaml:
    config: dict =  yaml.full_load(open_yaml)
    logger.info(f"loaded configurations file: {CONF_PATH}")

# Set data dir path
_default_data_dir_path = os.path.join(get_this_dir(), '../data')
DATA_DIR = Variable.get('data_dir', default_var=_default_data_dir_path)



# -------------------------------------------
# Setup bigquery client and table definitions
# -------------------------------------------


# Setup the bigquery client
PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']
KEY_PATH = config['cl_key_path']

_client: bigquery.Client = None

def get_client() -> bigquery.Client:
    """
    returns a bigquery client to the current project
    Returns:
        bigquery.Client: bigquery client
    """
    # check to see if the client has not been initialized
    global _client
    if _client is None:
        # initialize the client
        credentials = service_account.Credentials.from_service_account_file(KEY_PATH, 
                                                              scopes=["https://www.googleapis.com/auth/cloud-platform"])
        _client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        logger.info(f"successfully created bigquery client. project={PROJECT_NAME}")
    return _client


# Define table schemas
#avg cpi table
CPI_RATES =[
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("cpi", "FLOAT", mode="REQUIRED")]

#avg unemployment table
UNEMPLOYMENT_RATES = [
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("avg_unemp_per_year", "FLOAT", mode="REQUIRED")]

#avg stock table
AVG_STOCK_PRICE_PER_YEAR = [
            bigquery.SchemaField("stock_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("open", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("high", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("low", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("close", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("adj_close", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("volume", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("__index_level_0__", "FLOAT", mode="REQUIRED")]

# global dict to hold all tables schemas
TABLE_SCHEMAS = {
    'cpi_rates': 'CPI_RATES',
    'unemployment_rates': 'UNEMPLOYMENT_RATES',
    'avg_stock_price_per_year': 'AVG_STOCK_PRICE_PER_YEAR'
}


def create_table(table_name: str) -> None:
    
    assert table_name in TABLE_SCHEMAS, f"Table schema not found for table name: {table_name}"

    client = get_client()
    table_id = f"{PROJECT_NAME}.{DATASET_NAME}.{table_name}"

    try:
        table = client.get_table(table_id)      # table exists if this line doesn't raise exception
        client.delete_table(table)
        logger.info(f"dropped existed bigquery table: {table_id}")

        # wait a couple seconds before creating the table again
        time.sleep(2.0)
    except NotFound:
        # it's OK! table didn't exist
        pass
    # create the table
    schema = TABLE_SCHEMAS[table_name]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=False)
    logger.info(f"created bigquery table: {table_id}")


def load_table() -> None:
    
    job_config = bigquery.LoadJobConfig(
        df=pd.DataFrame, 
        client=bigquery.Client, 
        table_name=str, 
        schema=bigquery.SchemaField,
        create_disposition= 'CREATE_NEVER', 
        write_disposition=str = 'WRITE_TRUNCATE',
    )
    
    job = client.load_table_from_dataframe(df, destination=table_name, job_config=job_config)
    job.result()        # wait for the job to finish
        # get the number of rows inserted
    table = client.get_table(table_id)
    logger.info(f"inserted {table.num_rows} rows to {table_id}")




def wait_for_file():
    # define the file sensor...
    # wait for the airports file in the "data_fs" filesystem connection
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        poke_interval=15,                   # check every 15 seconds
        timeout=(30 * 60),                  # timeout after 30 minutes
        mode='poke',                        # mode: poke, reschedule
        filepath='/data'                    # file path to check (relative to fs_conn)
        fs_conn_id='data_fs',               # file system connection (root path)
    )

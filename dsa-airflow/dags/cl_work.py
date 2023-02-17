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
import google.auth


cpi_file = 'US-CPI.csv'
unemp_file = 'USUnemployment.csv'

cpi_complete = 'cpi.csv'
unemp_complete = 'unemp.csv'

DATA_DIR = '/data'

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

_default_config_path = os.path.join(get_this_dir(), './config.yml')
CONF_PATH = Variable.get('config_file', default_var=_default_config_path)
config: dict = {}
with open(CONF_PATH) as open_yaml:
    config: dict =  yaml.full_load(open_yaml)
    logger.info(f"loaded configurations file: {CONF_PATH}")

# # Set data dir path
# _default_data_dir_path = os.path.join(get_this_dir(), '../data')
# DATA_DIR = Variable.get('data_dir', default_var=_default_data_dir_path)



# -------------------------------------------
# Setup bigquery client and table definitions
# -------------------------------------------


# Setup the bigquery client
PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']

# KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
# credentials = service_account.Credentials.from_service_account_file(KEY_PATH, 
#                                                               scopes=["https://www.googleapis.com/auth/cloud-platform"])

_client : bigquery.Client = None
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
        _client = bigquery.Client(project=PROJECT_NAME)
        logger.info(f"successfully created bigquery client. project={PROJECT_NAME}")
    return _client

def check_bigquery_client():
    """
    task to see if we can successfully create a bigquery client
    """
    # check if $GOOGLE_APPLICATION_CREDENTIALS is set
    google_app_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if (google_app_creds is None) or (not os.path.exists(google_app_creds)):
        logger.warn("GOOGLE_APPLICATION_CREDENTIALS is not set properly!")
        logger.warn("You most likely have not edited the docker-compose.yaml file correctly. You must restart docker-compose after doing so.")
    # client from dsa_utils.table_definitions module
    logger.info("checking bigquery client")
    client = get_client()
    location = client.location
    logger.info(f"bigquery client is good. {location}")


def cpi_transformation():
    # read and rename cpi file / parse_dates=['Yearmon']
    cpi  = pd.read_csv(os.path.join(DATA_DIR, cpi_file), header=0)
    cpi = cpi.rename(columns={'Yearmon': 'year', 'CPI': 'cpi'})
    cpi[['month', 'date', 'year']] = cpi.year.str.split("-", expand=True)

    # calculate the avg yearly cpi rate
    cpi['avg_cpi_per_year'] = cpi.groupby('year')['cpi'].transform('mean')
    cpi[['month','year','date']] = cpi[['month','year','date']].astype('int')

    # write to file
    cpi.to_csv(os.path.join(DATA_DIR, 'cpi.csv'), header=True, index=False)

    print('File is created from cpi dataframe')


def unemp_transformation():
    # read and rename unemployment file
    unemp = pd.read_csv(os.path.join(DATA_DIR, unemp_file), header=0)
    unemp.columns = unemp.columns.str.lower()

    # calculate the avg yearly unemp. rate
    columns = ['jan',	'feb',	'mar',	'apr',	'may'	,'jun',	'jul',	'aug',	'sep',	'oct',	'nov',	'dec']
    unemp['avg_unemp_per_year']  = unemp[columns].mean(axis=1)

    unemp.to_csv(os.path.join(DATA_DIR, 'unemp.csv'), header=True, index=False)

    print('File is created from unemp dataframe')


# Define table schemas
#avg cpi table
CPI_RATES =[
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("cpi", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("date", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("avg_cpi_per_year", "FLOAT", mode="REQUIRED"),]

#avg unemployment table
UNEMPLOYMENT_RATES = [
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("jan", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("feb", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("mar", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("apr", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("may", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("jun", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("jul", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("aug", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("sep", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("oct", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("nov", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("dec", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("avg_unemp_per_year", "FLOAT", mode="REQUIRED")]

# global dict to hold all tables schemas
TABLE_SCHEMAS = {
    'cpi_rates': 'CPI_RATES',
    'unemployment_rates': 'UNEMPLOYMENT_RATES'
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


# global variable to hold data file
DATA_FILES = { 
    'cpi_rates': os.path.join(DATA_DIR, cpi_complete),
    'unemployment_rates': os.path.join(DATA_DIR, unemp_complete)}


# load table function 
def load_table(table_name: str):

    # make sure table_name is one of our data files
    assert table_name in DATA_FILES, f"Unknown table name: {table_name}"
    
    client = get_client()
    data_file = DATA_FILES[table_name]

    # check to see if data file exists
    assert os.path.exists(data_file), f"Missing data file: {data_file}"

    # insert data into bigquery
    table_id = f"{PROJECT_NAME}.{DATASET_NAME}.cpi_rates"

    # bigquery job config to load from a dataframe
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        create_disposition='CREATE_NEVER',
        write_disposition='WRITE_TRUNCATE',
        max_bad_records=100,
        ignore_unknown_values=True,
    )
    logger.info(f"loading bigquery {table_name} from file: {data_file}")
    with open(data_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    # wait for the job to complete
    job.result()
    # get the number of rows inserted
    table = client.get_table(table_id)
    logger.info(f"inserted {table.num_rows} rows to {table_id}")



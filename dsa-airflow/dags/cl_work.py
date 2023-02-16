import os
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import dag,task
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from collections import Counter

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
CPI_METADATA = {
    'cpi_rates': {
        'table_name': 'cpi_rates',
        'schema' : [
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("cpi", "FLOAT", mode="REQUIRED")]
    }}

UNEMP_METADATA = {
    'unemployment_rates' :{
        'table_name': 'unemployment_rates',
        'schema': [
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("avg_unemp_per_year", "FLOAT", mode="REQUIRED")]
    }}

STOCK_METADATA = {
    'avg_stock_price_per_year': {
        'table_name': 'avg_stock_price_per_year',
        'schema': [
            bigquery.SchemaField("stock_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("open", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("high", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("low", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("close", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("adj_close", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("volume", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("__index_level_0__", "FLOAT", mode="REQUIRED")]
    }}
"""
## Overview

This DAG will:
- Create BigQuery tables for: tornadoes
- Tables are dropped if they already exist
- Tables are loaded form CSV file in /data

## Setup

"""
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
# from google.oauth2 import service_account

# local imports
from phil_utils.utils import logger, config
from phil_utils.table_definitions import create_table, get_client
from phil_utils.table_loaders import load_table, DATA_FILES
from phil_utils.transformations import tornadoes_transformations

# # BigQuery credentials for proejct
# key_path = "/home/philiprobertovich/.creds/team-week-3.json"

# CREDENTIALS = service_account.Credentials.from_service_account_file(
#     key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
# )

# Pre-check tasks
# -----------------------------------------

def check_data_files():
    """
    task to first check if data files exist before doing anything else!
    """
    logger.info("checking data files")
    for filepath in DATA_FILES.values():
        if not os.path.exists(filepath):
            msg = f"Could not find source data file: {filepath}"
            logger.warn(msg)
            logger.warn("This is most likely because you haven't mounted the /data dir correctly in docker-compose.yaml. You must restart docker-compose after doing so.")
            raise FileNotFoundError(msg)


def check_bigquery_client():
    """
    task to see if we can successfully create a bigquery client
    """
    # check if $GOOGLE_APPLICATION_CREDENTIALS is set
    google_app_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if (google_app_creds is None) or (not os.path.exists(google_app_creds)):
        logger.warn("GOOGLE_APPLICATION_CREDENTIALS is not set properly!")
        logger.warn("You most likely have not edited the docker-compose.yaml file correctly. You must restart docker-compose after doing so.")
    # client from phil_utils.table_definitions module
    logger.info("checking bigquery client")
    client = get_client()
    location = client.location
    logger.info(f"bigquery client is good. bigquery location: {location}")


# DAG definition
# -----------------------------------------

with DAG(
    dag_id='tornadoes_etl',
    schedule_interval='@once',
    start_date=datetime.utcnow(),
    catchup=False,
    default_view='graph',
    is_paused_upon_creation=True,
    tags=['team-week-3', 'etl'],
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    }
) as dag:
    # dag's doc in markdown
    # setting it to this module's docstring defined at the very top of this file
    dag.doc_md = __doc__

    print(__file__)
    # pre-check task
    check_1 = PythonOperator(
        task_id='check_data_files',
        python_callable=check_data_files,
        doc_md=check_data_files.__doc__             # adding function docstring as task doc
    )
    check_2 = PythonOperator(
        task_id='check_bigquery_client',
        python_callable=check_bigquery_client,
        doc_md=check_bigquery_client.__doc__        # adding function docstring as task doc
    )

    # create an empty operator before branching out to create tables
    t1 = EmptyOperator(task_id='create_tables')

    table_names = ('tornadoes')

    # create a separate task for creating each table
    create_table_task = PythonOperator(
        task_id=f"create_tornadoes_table",
        python_callable=create_table,               # call the dsa_utils.table_definitions.create_table
        op_kwargs={'table_name': 'tornadoes'},       # arguments to create_table() function
        doc_md=create_table.__doc__                 # take function docstring
    )

    # create empty task to branch out transformations
    t2 = EmptyOperator(task_id='transform_files')

    # create transformation task
    transform_task = PythonOperator(
        task_id=f'transform_tornadoes_table',
        python_callable=tornadoes_transformations,
        doc_md=tornadoes_transformations
    )
    

    # create empty task to branch out to loading files
    t3 = EmptyOperator(task_id='load_files')

    # create a separate task for loading each table
    load_table_task = PythonOperator(
        task_id=f"load_tornadoes_table",
        python_callable=load_table,               # call the phil_utils.table_loaders.load_table
        op_kwargs={'table_name': 'tornadoes'},       # arguments to load_table() function
        doc_md=load_table.__doc__                 # take function docstring
        )

    # create empty task to branch back in
    done = EmptyOperator(task_id='done')

    check_1 >> check_2 >> t1 >> create_table_task >> t2 >> transform_task >> t3 >> load_table_task >> done

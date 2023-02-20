"""
## Overview

This DAG will:
- Create BigQuery tables for: _airports, airlines, routes, aircraft_
- Tables are dropped if they already exist
- Tables are loaded form CSV file in /data

## Setup

"""
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# local imports
from dsa_utils.utils import logger, config
from dsa_utils.table_definitions import create_table, get_client
from dsa_utils.table_loaders import load_table, DATA_FILES


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
    # client from dsa_utils.table_definitions module
    logger.info("checking bigquery client")
    client = get_client()
    location = client.location
    logger.info(f"bigquery client is good. bigquery location: {location}")


# DAG definition
# -----------------------------------------

with DAG(
    dag_id='dsa_load_tables',
    schedule_interval='@once',
    start_date=datetime.utcnow(),
    catchup=False,
    default_view='graph',
    is_paused_upon_creation=True,
    tags=['dsa', 'data-loaders'],
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

    table_names = ('airports', 'airlines', 'routes', 'aircraft')

    # create a separate task for creating each table
    create_tasks = []
    for table_name in table_names:
        task = PythonOperator(
            task_id=f"create_{table_name}_table",
            python_callable=create_table,               # call the dsa_utils.table_definitions.create_table
            op_kwargs={'table_name': table_name},       # arguments to create_table() function
            doc_md=create_table.__doc__                 # take function docstring
        )
        create_tasks.append(task)

    # create empty task to branch out to loading files
    t2 = EmptyOperator(task_id='load_files')

    # create a separate task for loading each table
    load_tasks = []
    for table_name in table_names:
        task = PythonOperator(
            task_id=f"load_{table_name}_table",
            python_callable=load_table,               # call the dsa_utils.table_loaders.load_table
            op_kwargs={'table_name': table_name},       # arguments to load_table() function
            doc_md=load_table.__doc__                 # take function docstring
        )
        load_tasks.append(task)

    # create empty task to branch back in
    done = EmptyOperator(task_id='done')

    check_1 >> check_2 >> t1 >> create_tasks >> t2 >> load_tasks >> done

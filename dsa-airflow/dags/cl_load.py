import os
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago 
import pandas as pd
from airflow import DAG
from airflow.decorators import dag,task
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from cl_work import get_client, config, cpi_transformation, unemp_transformation, load_table, create_table

PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']
KEY_PATH = config['cl_key_path']
TABLE_ID = 'stocks'

default_args = {
    'start_date': days_ago(2), # The start date for DAG running. This function allows us to set the start date to two days ago
    'schedule_interval': timedelta(days=1), # How often our DAG will run. After the start_date, airflow waits for the schedule_interval to pass then triggers the DAG run
    'retries': 1, # How many times to retry in case of failure
    'retry_delay': timedelta(minutes=5), # How long to wait before retrying
}

# instantiate a DAG!
with DAG(
    'ETL pipeline', 
    description='A DAG to do transformation once files are detected',
    default_args=default_args,
) as dag:

  check_bq_client = PythonOperator(
    task_id = "check_bq_client",
    python_callable=get_client
  )

  wait_for_files = FileSensor(
    task_id='wait_for_files',
    poke_interval=15,                   
    timeout=(30 * 60),                  
    mode='poke',                        
    filepath='/data',                    
    fs_conn_id='data_fs'
  )

  cpi_transform=PythonOperator(
    task_id='cpi_transform', 
    python_callable=cpi_transformation
  )

  unemp_transform=PythonOperator(
    task_id='unemp_transform',
    python_callable=unemp_transformation
  )

  create_tables = PythonOperator(
    task_id='create_tables',
    python_callable=create_table
  )

  load_tables=PythonOperator(
    task_id='load_tables',
    python_callable=load_table
  )



check_bq_client >> wait_for_files >> [cpi_transformation ] >> create_table >> load_table
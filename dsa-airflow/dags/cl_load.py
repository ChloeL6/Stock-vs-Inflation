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
from cl_work import check_bigquery_client, config, cpi_transformation, unemp_transformation, load_table, create_table

PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']
KEY_PATH = config['cl_key_path']

default_args = {
    'start_date': days_ago(2), # The start date for DAG running. This function allows us to set the start date to two days ago
    'schedule_interval': timedelta(days=1), # How often our DAG will run. After the start_date, airflow waits for the schedule_interval to pass then triggers the DAG run
    'retries': 1, # How many times to retry in case of failure
    'retry_delay': timedelta(minutes=5), # How long to wait before retrying
}

# instantiate a DAG!
with DAG(
    'ETL_pipeline', 
    description='A DAG to do transformation once files are detected',
    default_args=default_args,
) as dag:

  check_bq_client = PythonOperator(
    task_id = "check_bq_client",
    python_callable=check_bigquery_client
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

  t1=DummyOperator(task_id='create_tables')


  # create a separate task for creating each table
  table_names = ['cpi_rates', 'unemployment_rates']

  create_tasks = []

  for table_name in  table_names:
      task = PythonOperator(
        task_id=f'create_{table_name}table',
        python_callable=create_table,
        op_kwargs={'table_name': table_name},
        doc_md=create_table.__doc__                 # take function docstring
        )
      
      create_tasks.append(task)

  # create empty task to branch out to loading files
  t2=DummyOperator(task_id='load_files')

  # create a separate task for loading files
  load_tasks = []
  for table_name in table_names:
      task = PythonOperator(
        task_id=f'load_{table_name}_table',
        python_callable=load_table,
        op_kwargs={'table_name': table_name},
        doc_md=create_table.__doc__ 
     )
      
      load_tasks.append(task)

  # create empty task to branch back in
  done = DummyOperator(task_id='done')

check_bq_client >> wait_for_files >> [cpi_transform, unemp_transform] >> t1 >> create_tasks >> t2 >> load_tasks >> done
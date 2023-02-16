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
from cl_work import get_client


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

  wait_for_files = PythonOperator(
    task_id='wait_for_file',
    python_callable=wait_for_files
  )

  wait_for_stocks_file = PythonOperator()

  cpi_transformation=PythonOperator()
  unemp_transformation=PythonOperator()
  stock_transformation=PythonOperator()

  create_table = PythonOperator()

  load_table=PythonOperator()



check_bq_client >> [wait_for_files, wait_for_stocks_file] >> [cpi_transformation , , ] >> create_table >> load_table
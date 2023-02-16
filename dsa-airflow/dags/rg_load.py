import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.models import Variable
import yaml

# local imports
from rg_work import create_dataset, create_stocks_table, data_dir, config, transform

data_file_names1 = ['AAPL', 'ADBE','AMZN', 'CRM', 'CSCO', 'GOOGL', 'IBM']
data_file_names2 = ['INTC','META','MSFT','NFLX','NVDA','ORCL','TSLA']


table_names = ['stocks']


# DAG definition
# -----------------------------------------

with DAG(
    dag_id='ETL_stocks_table_load',
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

    check_1 = []
    for file in data_file_names1:
        check = FileSensor(
            task_id=f'wait_for_{file}_file',
            poke_interval=15,                   # check every 15 seconds
            timeout=(30 * 60),                  # timeout after 30 minutes
            mode='poke',                        # mode: poke, reschedule
            filepath=f'{file}.csv',        # file path to check  
            fs_conn_id='data_fs'   
        )
        check_1.append(check)
    
    check_1_com = EmptyOperator(task_id='tech_stocks_group_1')

    check_2 = []
    for file in data_file_names2:
        check = FileSensor(
            task_id=f'wait_for_{file}_file',
            poke_interval=15,                   # check every 15 seconds
            timeout=(30 * 60),                  # timeout after 30 minutes
            mode='poke',                        # mode: poke, reschedule
            filepath=f'{file}.csv',        # file path to check  
            fs_conn_id='data_fs'   
        )
        check_2.append(check)
    
    check_2_com = EmptyOperator(task_id='tech_stocks_group_2')

    transform_task = PythonOperator(
        task_id='transformations',
        python_callable = transform,
        doc_md = transform.__doc__        # adding function docstring as task doc
    )

    parquet_task = EmptyOperator(task_id='create_parquet_file')

    t0 = PythonOperator(
        task_id='create_dataset',
        python_callable = create_dataset,
        doc_md = create_dataset.__doc__        # adding function docstring as task doc
    )

    
    stocks_table_task = PythonOperator(
        task_id=f"load_stocks_table",
        python_callable=create_stocks_table,               # call the dsa_utils.table_definitions.create_table
        doc_md=create_stocks_table.__doc__                 # take function docstring
    )

    check_1 >> check_1_com >> check_2 >> check_2_com >> transform_task >> parquet_task >> t0 >> stocks_table_task
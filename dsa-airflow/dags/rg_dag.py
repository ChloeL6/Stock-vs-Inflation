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
from rg_work import create_dataset, create_stocks_table, data_dir, config

data_file_names = ['AAPL', 'ADBE','AMZN', 'CRM', 'CSCO', 'GOOGL', 'IBM','INTC','META','MSFT','NFLX','NVDA','ORCL','TSLA']
DATA_FILES = {file: os.path.join(data_dir, file+'.csv') for file in data_file_names}
table_names = ['stocks']


# DAG definition
# -----------------------------------------

with DAG(
    dag_id='dsa_tables_load',
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

    check_0 = []
    for file in DATA_FILES.keys():
        check = FileSensor(
            task_id=f'wait_for_{file}_file',
            poke_interval=15,                   # check every 15 seconds
            timeout=(30 * 60),                  # timeout after 30 minutes
            mode='poke',                        # mode: poke, reschedule
            filepath=f'{file}.csv',        # file path to check  
            fs_conn_id='data_fs'   
        )
        check_0.append(check)
    
    t0 = PythonOperator(
        task_id='create_dataset',
        python_callable = create_dataset,
        doc_md = create_dataset.__doc__        # adding function docstring as task doc
    )


    stocks_table_task = PythonOperator(
        task_id=f"create_stocks_table",
        python_callable=create_stocks_table,               # call the dsa_utils.table_definitions.create_table
        doc_md=create_stocks_table.__doc__                 # take function docstring
    )

    check_0 >> t0 >> stocks_table_task
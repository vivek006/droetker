from datetime import timedelta, datetime
import os
import pytz

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from droetker.src import extractor

dag = DAG(
    'demo_data_fetch',
    default_args={
        'owner': 'airflow',
        "max_active_runs": 1,
        'depends_on_past': False,
        'email': ['vivek.ranjan@zenport.io'],
        'email_on_failure': False,
        'email_on_retry': False,
        'email_on_success': False,
        'retry_delay': timedelta(minutes=30)
    },
    schedule_interval='0 21 * * *',
    start_date=datetime(2022, 10, 17),
    catchup=False
)


new_file_found = PythonOperator(
    task_id='get_process_file_details',
    python_callable=extractor.extract_data,
    do_xcom_push=True,
    provide_context=True,
    dag=dag,
)

new_file_found

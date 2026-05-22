from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Tell Airflow where to find our scripts
sys.path.insert(0, '/opt/airflow/scripts')
sys.path.insert(0, '/opt/airflow/dags')

from validate import validate_data
from transform import transform_data
from download import download_data

# File paths inside Docker
RAW_FILE = '/opt/airflow/data/raw/cms_latest.csv'
PROCESSED_FILE = '/opt/airflow/data/processed/clean_claims.csv'

# Default settings for the DAG
default_args = {
    'owner': 'sajjan',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

# Define the DAG
with DAG(
    dag_id='healthcare_claims_etl',
    default_args=default_args,
    description='ETL pipeline for CMS Medicare claims data',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['healthcare', 'etl', 'cms'],
) as dag:

    # Task 0: Download fresh data from CMS
    download_task = PythonOperator(
        task_id='download_cms_data',
        python_callable=download_data,
        op_kwargs={'output_filepath': RAW_FILE},
    )

    # Task 1: Validate
    validate_task = PythonOperator(
        task_id='validate_raw_data',
        python_callable=validate_data,
        op_kwargs={'filepath': RAW_FILE},
    )

    # Task 2: Transform
    transform_task = PythonOperator(
        task_id='transform_and_clean_data',
        python_callable=transform_data,
        op_kwargs={
            'input_filepath': RAW_FILE,
            'output_filepath': PROCESSED_FILE,
        },
    )

    # Task 3: Confirm
    def confirm_load(**kwargs):
        import pandas as pd
        df = pd.read_csv(PROCESSED_FILE)
        print(f"Pipeline complete. {len(df)} rows loaded.")
        print(df.head())

    load_task = PythonOperator(
        task_id='confirm_load',
        python_callable=confirm_load,
    )

    # Set the order
    download_task >> validate_task >> transform_task >> load_task
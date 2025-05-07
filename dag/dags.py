"""
Airflow DAG for FincaRaiz real estate data pipeline.

This DAG orchestrates the extraction, transformation, and loading of real estate data
from FincaRaiz website for multiple cities in Colombia.
"""

from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator

from func import web_scrapping, get_trigger_day, save_in_db

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'email': ['example@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Get current date for file naming
current_date = datetime.now().strftime("%d-%m-%Y")

# Define the DAG
with DAG(
    dag_id='FincaRaiz',
    default_args=default_args,
    description='Real estate data pipeline for FincaRaiz',
    tags=['real_estate', 'fincaraiz']
) as dag:
    
    # List of cities to process
    cities = [
        'cali/valle-del-cauca',
        'bogota/bogota-dc', 
        'medellin/antioquia'
    ]

    # Task to extract data from FincaRaiz website
    extract_data = PythonOperator.partial(
        task_id='extract_data',
        python_callable=web_scrapping
    ).expand(
        op_kwargs=[{'city': city} for city in cities]
    )

    # Task to save extracted data to database
    save_db = PythonOperator.partial(
        task_id='save_db',
        python_callable=save_in_db
    ).expand(
        op_kwargs=[{'city': city} for city in cities]
    )

    # Task to get trigger information
    trigger_info = PythonOperator(
        task_id='trigger_info',
        python_callable=get_trigger_day
    )

    # Task to upload data to Azure Blob Storage
    upload_to_azure = LocalFilesystemToWasbOperator(
        task_id="upload_to_azure",
        file_path=f"/home/jhona/housing-retail-pipeline/data/date/{current_date}.csv",
        container_name="triggerdate",
        blob_name=f"date/{current_date}.csv",
        wasb_conn_id="azure_blob_storage",
    )

    # Define task dependencies
    extract_data >> save_db >> trigger_info >> upload_to_azure
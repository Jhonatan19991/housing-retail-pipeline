from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 2),  # Update the start date to today or an appropriate date
    'email': ['example@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    'Finca Raiz',
    default_args=default_args,
    description='workflow',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:
    
    cities = ['cali/valle-del-cauca','bogota/bogota-dc', 'medellin/antioquia']

    mapped_task = PythonOperator.partial(
        task_id='extract_data',
        python_callable=float
    ).expand(
        op_kwargs=[{'city': city} for city in cities]
    )
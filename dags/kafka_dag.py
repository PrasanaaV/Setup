from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka_functions import kafka_fct

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'kafka_dag',
    default_args=default_args,
    description='Extraction DAG and send to Kafka (RealTime)',
    schedule_interval='@once',
)

python_task1 = PythonOperator(
    task_id='kafka_fct',
    python_callable=kafka_fct,
    dag=dag,
)
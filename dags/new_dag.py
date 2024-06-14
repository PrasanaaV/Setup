from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
    'spark_dag',
    default_args=default_args,
    description='A simple DAG to execute a Python script',
    schedule_interval='@once',
)

start = EmptyOperator(task_id='start', dag=dag)

spark_job = SparkSubmitOperator(
    task_id='spark_job',
    conn_id='spark-conn',
    application='jobs/processing.py',
    dag=dag,
)
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 3),
    'retries': 1,
}

# Define the DAG
dag = DAG('preprocessing', 
          default_args=default_args,
          description='Example DAG with SparkSubmitOperator', 
          schedule_interval='@once',
          start_date=datetime(2024, 5, 3),
          catchup=False)

spark_submit_task1 = SparkSubmitOperator(
    application='jobs/processing.py',
    conn_id='spark-conn',
    task_id='spark',
)

spark_submit_task2 = SparkSubmitOperator(
    application='jobs/kafka_watch.py',
    conn_id='spark-conn',
    task_id='spark',
)

start = EmptyOperator(task_id='start', dag=dag)
start >> spark_submit_task1 >> spark_submit_task2
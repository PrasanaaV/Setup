from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from stations_functions import fetch_and_insert_into_elasticsearch_emplacement_stations, fetch_and_insert_into_elasticsearch_perim, elastic_metro_station

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'extraction_loading_es',
    default_args=default_args,
    description='Extraction DAG and Loading into ElasticSearch',
    schedule_interval='@once',
)

python_task1 = PythonOperator(
    task_id='insert_into_elasticsearch_perimetre',
    python_callable=fetch_and_insert_into_elasticsearch_perim,
    dag=dag,
)

# python_task2 = PythonOperator(
#     task_id='elastic_metro_station',
#     python_callable=elastic_metro_station,
#     dag=dag,
# )

# python_task3 = PythonOperator(
#     task_id='fetch_and_insert_into_elasticsearch_emplacement_stations',
#     python_callable=fetch_and_insert_into_elasticsearch_emplacement_stations,
#     dag=dag,
# )

python_task1
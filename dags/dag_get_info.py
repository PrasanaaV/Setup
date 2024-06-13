# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from processing import precossing_fct

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 5, 3),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
# }

# dag = DAG(
#     'preprocessing_example',
#     default_args=default_args,
#     description='Example DAG with data preprocessing based on search query',
#     schedule_interval='None',
#     catchup=False,
# )

# task_preprocess_data = PythonOperator(
#     task_id='preprocess_data_task',
#     python_callable=precossing_fct,
#     provide_context=True,
#     dag=dag,
# )
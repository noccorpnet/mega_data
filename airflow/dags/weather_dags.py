import subprocess
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'opt/data-engineer/weather-project/scripts')))

sys.path.append('/opt/data-engineer/weather-project/scripts')

from weather_data import main as fetch_weather_data
from get_engine import get_engine
from geolocation import main as fetch_geolocation_data


# def run_dbt_transformations():
#     try:
#         subprocess.run(["dbt", "run", "--profiles-dir", "/opt/data-engineer/weather-project/dbt/weather_dbt"], check=True)
#         print("DBT transformations completed successfully.")
#     except subprocess.CalledProcessError as e:
#         print(f"An error occurred while running DBT transformations: {e}")
        

default_args = {
    'owner': 'Hendi Irwanto',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch and process weather data, geolocation data, load them into the database and transform using DBt.',
    schedule_interval='@daily',
    catchup=False,
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

fetch_geolocation_task = PythonOperator(
    task_id='fetch_geolocation_data',
    python_callable=fetch_geolocation_data,
    dag=dag,
)

fetch_weather_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

dbt_transform_task = BashOperator(
    task_id='run_dbt_transformations',
    bash_command='dbt run --project-dir /opt/data-engineer/weather-project/dbt/weather_dbt --profiles-dir /opt/data-engineer/weather-project/dbt/weather_dbt/.dbt',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

start_task >> fetch_geolocation_task >> fetch_weather_task >> dbt_transform_task >> end_task
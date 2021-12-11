from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from WeatherMetrics.cities import CITIES
from WeatherMetrics.domain import get_temperature_data_open_meteo, save_temperature_data

default_args: Dict[str, Any] = dict(
    owner='Leandro Szikora',
    depends_on_past=True,
    retries=1,
    retry_delay=timedelta(minutes=5)
)

config: Dict[str, Any] = dict(
    dag_id='WeatherMetrics',
    schedule_interval='@daily',
    start_date=datetime(2021, 12, 10),
    catchup=False,
    tags=['weather', 'data_eng'],
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    default_args=default_args
)

with DAG(**config) as dag:
    for city in CITIES:
        city_name: str = '_'.join(city['city']['name'].lower().split(' '))

        get_temperature_task: PythonOperator = PythonOperator(
            task_id=f'get_temp_data_{city_name}',
            python_callable=get_temperature_data_open_meteo,
            op_kwargs=dict(
                ds="{{ ds }}",
                base_url="{{ var.value.open_meteo_url }}",
                lon=city['city']['coord']['lon'],
                lat=city['city']['coord']['lat'],
                city_id=city['id']
            ),
            do_xcom_push=True
        )
        
        save_temperature_task: PythonOperator = PythonOperator(
            task_id=f'save_temp_data_{city_name}',
            python_callable=save_temperature_data,
            op_kwargs=dict(
                city_name=city_name
            ),
            provide_context=True
        )

        get_temperature_task >> save_temperature_task


from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from WeatherMetrics.cities import CITIES
from WeatherMetrics.TemperatureMetrics.domain import INSERT_INTO_STMT
from WeatherMetrics.weather_lib.operators.open_meteo import OpenMeteoMetricsOperator

default_args: Dict[str, Any] = dict(
    owner='Leandro Szikora',
    depends_on_past=True,
    retries=1,
    retry_delay=timedelta(minutes=5)
)

config: Dict[str, Any] = dict(
    dag_id='TemperatureMetrics',
    schedule_interval='@daily',
    start_date=datetime(2022, 9, 15),
    catchup=False,
    tags=['weather', 'data_eng', 'test-tag'],
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    default_args=default_args
)

with DAG(**config) as dag:
    for city in CITIES:
        city_name: str = '_'.join(city['city']['name'].lower().split(' '))

        with TaskGroup(group_id=f'process_{city_name}') as city_group:
            get_temperature_task = OpenMeteoMetricsOperator(
                task_id=f'get_temperature_data_{city_name}',
                pool='weather_pool',
                priority_weight=city['priority'],
                city_data=city,
                frequency='daily',
                metrics=['temperature_2m_max', 'temperature_2m_min'],
                metrics_mapper=dict(
                    temperature_2m_max='max',
                    temperature_2m_min='min'
                ),
                http_conn_id='open_meteo'
            )

            delete_ds_city_values = PostgresOperator(
                task_id=f'delete_temperature_data_{city_name}',
                pool='weather_pool',
                postgres_conn_id='postgres_default',
                sql="DELETE FROM temperature WHERE id = {{params.id}} AND date = '{{ds}}'",
                params={'id': city['id']}
            )

            save_weather_data = PostgresOperator(
                task_id=f'save_temperature_data_{city_name}',
                postgres_conn_id='postgres_default',
                sql=INSERT_INTO_STMT % {'name': city_name}
            )

            get_temperature_task >> delete_ds_city_values >> save_weather_data

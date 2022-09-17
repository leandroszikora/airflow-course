from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from WeatherMetrics.cities import CITIES
from WeatherMetrics.HumidityMetrics.domain import INSERT_INTO_STMT
from WeatherMetrics.weather_lib.operators.open_meteo import OpenMeteoMetricsOperator

default_args: Dict[str, Any] = dict(
    owner='Leandro Szikora',
    depends_on_past=True,
    retries=1,
    retry_delay=timedelta(minutes=5)
)

config: Dict[str, Any] = dict(
    dag_id='HumidityMetrics',
    schedule_interval='0 3 * * *',
    start_date=datetime(2021, 12, 10),
    catchup=False,
    tags=['weather', 'data_eng'],
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    default_args=default_args
)

with DAG(**config) as dag:
    cities_process: List[Any] = []

    temperature_sensor: ExternalTaskSensor = ExternalTaskSensor(
        task_id='wait_temperature_metrics_dag',
        external_dag_id='TemperatureMetrics',
        execution_delta=timedelta(hours=1),
        mode='reschedule',
        timeout=60 * 60 * 2,
        poke_interval=60 * 2
    )

    for city in CITIES:
        city_name: str = '_'.join(city['city']['name'].lower().split(' '))

        with TaskGroup(group_id=f'process_{city_name}') as city_group:
            get_humidity_task = OpenMeteoMetricsOperator(
                task_id=f'get_humidity_data_{city_name}',
                pool='weather_pool',
                priority_weight=city['priority'],
                city_data=city,
                frequency='hourly',
                metrics=['relativehumidity_2m'],
                metrics_mapper=dict(
                    relativehumidity_2m='relative_humidity'
                ),
                http_conn_id='open_meteo'
            )

            delete_ds_humidity_values = PostgresOperator(
                task_id=f'delete_humidity_data_{city_name}',
                pool='weather_pool',
                postgres_conn_id='postgres_default',
                sql="DELETE FROM humidity WHERE id = {{params.id}} AND date = '{{ds}}'",
                params={'id': city['id']}
            )

            save_weather_data = PostgresOperator(
                task_id=f'save_humidity_data_{city_name}',
                postgres_conn_id='postgres_default',
                sql=INSERT_INTO_STMT % {'name': city_name}
            )

            get_humidity_task >> delete_ds_humidity_values >> save_weather_data

            cities_process.append(get_humidity_task)

    temperature_sensor >> cities_process

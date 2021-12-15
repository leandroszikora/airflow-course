from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.decorators import task_group, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from TemperatureMetrics.cities import CITIES
from TemperatureMetrics.domain import get_temperature_data_open_meteo, INSERT_INTO_STMT

default_args: Dict[str, Any] = dict(
    owner='Leandro Szikora',
    depends_on_past=True,
    retries=1,
    retry_delay=timedelta(minutes=5)
)

config: Dict[str, Any] = dict(
    dag_id='TemperatureMetrics',
    schedule_interval='@daily',
    start_date=datetime(2021, 12, 10),
    catchup=False,
    tags=['weather', 'data_eng'],
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    default_args=default_args
)

with DAG(**config) as dag:
    trigger_humidity_dag_task = TriggerDagRunOperator(
        task_id='trigger_humidity_dag',
        trigger_dag_id='HumidityMetrics',
        wait_for_completion=True,
        poke_interval=120
    )

    for city in CITIES:
        city_name: str = '_'.join(city['city']['name'].lower().split(' '))

        @task_group(group_id=f'process_{city_name}')
        def city_group():
            @task(task_id=f'get_temperature_data_{city_name}', pool='weather_pool', priority_weight=city['priority'])
            def get_temperature_task(url: str, city_param: Dict[str, Any], **kwargs) -> Dict[str, Any]:
                ds: str = kwargs.get('ds')
                city_id: str = city_param['id']
                lon: float = city_param['city']['coord']['lon']
                lat: float = city_param['city']['coord']['lat']
                return get_temperature_data_open_meteo(ds, url, lon, lat, city_id)

            delete_ds_city_values = PostgresOperator(
                task_id=f'delete_temperature_data_{city_name}',
                pool='weather_pool',
                postgres_conn_id='postgres_default',
                sql="DELETE FROM weather WHERE id = {{params.id}} AND date = '{{ds}}'",
                params={'id': city['id']}
            )

            save_weather_data = PostgresOperator(
                task_id=f'save_temperature_data_{city_name}',
                postgres_conn_id='postgres_default',
                sql=INSERT_INTO_STMT % {'name': city_name}
            )

            temperature_data: Dict[str, Any] = get_temperature_task("{{ var.value.open_meteo_url }}", city)
            temperature_data >> delete_ds_city_values >> save_weather_data >> trigger_humidity_dag_task


        city_group()

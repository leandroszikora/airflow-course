from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.decorators import task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator

from TemperatureMetrics.cities import CITIES
from HumidityMetrics.domain import get_humidity_data_open_meteo, INSERT_INTO_STMT

default_args: Dict[str, Any] = dict(
    owner='Leandro Szikora',
    depends_on_past=True,
    retries=1,
    retry_delay=timedelta(minutes=5)
)

config: Dict[str, Any] = dict(
    dag_id='HumidityMetrics',
    schedule_interval='0 1 * * *',
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


        @task_group(group_id=f'process_{city_name}', dag=dag)
        def city_group():
            @dag.task(task_id=f'get_humidity_data_{city_name}', pool='weather_pool', priority_weight=city['priority'])
            def get_humidity_task(url: str, city_param: Dict[str, Any], **kwargs) -> Dict[str, Any]:
                ds: str = kwargs.get('ds')
                city_id: str = city_param['id']
                lon: float = city_param['city']['coord']['lon']
                lat: float = city_param['city']['coord']['lat']
                return get_humidity_data_open_meteo(ds, url, lon, lat, city_id)

            delete_ds_humidity_values = PostgresOperator(
                task_id=f'delete_humidity_data_{city_name}',
                pool='weather_pool',
                postgres_conn_id='postgres_default',
                sql="DELETE FROM weather WHERE id = {{params.id}} AND date = '{{ds}}'",
                params={'id': city['id']}
            )

            save_weather_data = PostgresOperator(
                task_id=f'save_humidity_data_{city_name}',
                postgres_conn_id='postgres_default',
                sql=INSERT_INTO_STMT % {'name': city_name}
            )

            humidity_data: Dict[str, Any] = get_humidity_task("{{ var.value.open_meteo_url }}", city)
            humidity_data >> delete_ds_humidity_values >> save_weather_data


        city_group()

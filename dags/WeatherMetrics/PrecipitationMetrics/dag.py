from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.decorators import task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from WeatherMetrics.cities import CITIES
from WeatherMetrics.PrecipitationMetrics.domain import get_precipitation_data_open_meteo, check_if_rain, \
    INSERT_INTO_STMT, UPDATE_RAIN_FLAG

default_args: Dict[str, Any] = dict(
    owner='Leandro Szikora',
    depends_on_past=True,
    retries=1,
    retry_delay=timedelta(minutes=5)
)

config: Dict[str, Any] = dict(
    dag_id='PrecipitationMetrics',
    schedule_interval='0 1 * * *',
    start_date=datetime(2021, 12, 13),
    catchup=False,
    tags=['weather', 'data_eng'],
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    default_args=default_args
)

with DAG(**config) as dag:
    cities_process: List[Any] = []

    precipitation_sensor: ExternalTaskSensor = ExternalTaskSensor(
        task_id='wait_humidity_metrics_dag',
        external_dag_id='HumidityMetrics',
        timeout=60 * 60 * 2
    )

    for city in CITIES:
        city_name: str = '_'.join(city['city']['name'].lower().split(' '))


        @task_group(group_id=f'process_{city_name}', dag=dag)
        def city_group():
            @dag.task(task_id=f'get_precipitation_data_{city_name}', pool='weather_pool',
                      priority_weight=city['priority'])
            def get_precipitation_task(url: str, city_param: Dict[str, Any], **kwargs) -> Dict[str, Any]:
                ds: str = kwargs.get('ds')
                city_id: str = city_param['id']
                lon: float = city_param['city']['coord']['lon']
                lat: float = city_param['city']['coord']['lat']
                return get_precipitation_data_open_meteo(ds, url, lon, lat, city_id)

            delete_ds_precipitation_values = PostgresOperator(
                task_id=f'delete_precipitation_data_{city_name}',
                pool='weather_pool',
                postgres_conn_id='postgres_default',
                sql="DELETE FROM precipitation WHERE id = {{params.id}} AND date = '{{ds}}'",
                params={'id': city['id']}
            )

            save_weather_data = PostgresOperator(
                task_id=f'save_precipitation_data_{city_name}',
                postgres_conn_id='postgres_default',
                sql=INSERT_INTO_STMT % {'name': city_name}
            )

            check_precipitation_value = BranchPythonOperator(
                task_id=f'check_if_rain_data_{city_name}',
                python_callable=check_if_rain,
                op_kwargs=dict(city_name=city_name),
                provide_context=True
            )

            update_rain_flag = PostgresOperator(
                task_id=f'update_did_rain_flag_{city_name}',
                postgres_conn_id='postgres_default',
                sql=UPDATE_RAIN_FLAG,
                params=dict(city_id=city['id'])
            )

            skip_update_rain_flag = DummyOperator(task_id=f'skip_update')

            precipitation_data: Dict[str, Any] = get_precipitation_task("{{ var.value.open_meteo_url }}", city)

            (
                    precipitation_data >> delete_ds_precipitation_values >> save_weather_data >>
                    check_precipitation_value >> [update_rain_flag, skip_update_rain_flag]
            )

            cities_process.append(precipitation_data)


        city_group()

    precipitation_sensor >> cities_process

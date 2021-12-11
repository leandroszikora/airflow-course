from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.decorators import dag, task, task_group

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


@dag(**config)
def temperature_dag():
    for city in CITIES:
        city_name: str = '_'.join(city['city']['name'].lower().split(' '))

        @task_group(group_id=f'process_{city_name}')
        def city_group():
            @task(task_id=f'get_weather_data_{city_name}')
            def get_temperature_task(url: str, city_param: Dict[str, Any], **kwargs) -> Dict[str, Any]:
                ds: str = kwargs.get('ds')
                city_id: str = city_param['id']
                lon: float = city_param['city']['coord']['lon']
                lat: float = city_param['city']['coord']['lat']
                return get_temperature_data_open_meteo(ds, url, lon, lat, city_id)

            @task(task_id=f'save_temp_data_{city_name}')
            def save_temperature_task(temperature: Dict[str, Any]):
                save_temperature_data(temperature)

            temperature_data: Dict[str, Any] = get_temperature_task("{{ var.value.open_meteo_url }}", city)
            save_temperature_task(temperature_data)

        city_group()


temperature_dag: DAG = temperature_dag()

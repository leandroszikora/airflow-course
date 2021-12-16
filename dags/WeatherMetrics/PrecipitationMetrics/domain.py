"""
Functions used for manage open meteo data about precipitations
"""
import requests

from airflow.models.taskinstance import TaskInstance
from typing import Dict, Any

INSERT_INTO_STMT: str = """
INSERT INTO precipitation (date, id, milliliters, hours) VALUES (
    '{{ ti.xcom_pull(task_ids="process_%(name)s.get_precipitation_data_%(name)s")['time'] }}', 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_precipitation_data_%(name)s")['id'] }}, 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_precipitation_data_%(name)s")['milliliters'] }}::float,
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_precipitation_data_%(name)s")['hours'] }}::float
)
"""

UPDATE_RAIN_FLAG: str = """
UPDATE humidity SET did_rain = True WHERE id = {{ params.city_id }} AND date = '{{ ds }}'
"""


def get_precipitation_data_open_meteo(ds: str, base_url: str, lon: float, lat: float, city_id: str) -> Dict[str, Any]:
    """
    Makes an HTTP GET action to retrieve the data of the city precipitations
    :param ds: date of the temperature to retrieve
    :param base_url: open meteo url
    :param lon: longitude of the city
    :param lat: latitude of the city
    :param city_id: id of the city
    :return: dict with temperature data
    """
    params: Dict[str, Any] = {
        'latitude': lat,
        'longitude': lon,
        'daily': 'precipitation_sum,precipitation_hours',
        'timezone': 'UTC',
        'past_days': 2
    }
    response: requests.Response = requests.get(base_url, params='&'.join("%s=%s" % (k, v) for k, v in params.items()))
    response: Dict[str, Any] = response.json()
    index: int = response['daily']['time'].index(ds)

    return {
        'time': ds,
        'id': city_id,
        'milliliters': response['daily']['precipitation_sum'][index],
        'hours': response['daily']['precipitation_hours'][index]
    }


def check_if_rain(ti: TaskInstance, city_name: str) -> str:
    """
    Evaluates milliliters parameter and returns task id to update rain flag if true
    :param ti: context of the task instance
    :param city_name: name of the city
    :return: task id
    """
    precipitation_data = ti.xcom_pull(
        key='return_value',
        task_ids=[f'process_{city_name}.get_precipitation_data_{city_name}']
    )
    milliliters = float(precipitation_data[0]['milliliters'])
    return (f'process_{city_name}.update_did_rain_flag_{city_name}'
            if float(milliliters) > 0.0
            else f'process_{city_name}.skip_update')

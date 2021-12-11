"""

"""
import requests

from airflow.models.taskinstance import TaskInstance
from typing import Dict, Any

url: str = "https://api.open-meteo.com/v1/forecast"

INSERT_INTO_STMT: str = """
INSERT INTO weather (date, id, min, max) VALUES (
    '{{ ti.xcom_pull(task_ids="process_%(name)s.get_weather_data_%(name)s")['time'] }}', 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_weather_data_%(name)s")['id'] }}, 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_weather_data_%(name)s")['min'] }}, 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_weather_data_%(name)s")['max'] }}
)
"""


def get_temperature_data_open_meteo(ds: str, base_url: str, lon: float, lat: float, city_id: str) -> Dict[str, Any]:
    """

    :param ds:
    :param base_url:
    :param lon:
    :param lat:
    :param city_id:
    :return:
    """
    params: Dict[str, Any] = {
        'latitude': lat,
        'longitude': lon,
        'daily': 'temperature_2m_max,temperature_2m_min',
        'timezone': 'UTC',
        'past_days': 2
    }
    response: requests.Response = requests.get(base_url, params='&'.join("%s=%s" % (k, v) for k, v in params.items()))
    response: Dict[str, Any] = response.json()
    index: int = response['daily']['time'].index(ds)

    return {
        'time': ds,
        'id': city_id,
        'min': response['daily']['temperature_2m_min'][index],
        'max': response['daily']['temperature_2m_max'][index]
    }


def save_temperature_data(ti: TaskInstance, city_name: str) -> None:
    """

    :param ti:
    :param city_name:
    :return:
    """
    temperature_data: Dict[str, Any] = ti.xcom_pull(key='return_value', task_ids=[f'get_temp_data_{city_name}'])
    print(temperature_data)

"""
Functions used for manage open meteo data about temperature
"""
import requests

from typing import Dict, Any

url: str = "https://api.open-meteo.com/v1/forecast"

INSERT_INTO_STMT: str = """
INSERT INTO temperature (date, id, min, max) VALUES (
    '{{ ti.xcom_pull(task_ids="process_%(name)s.get_temperature_data_%(name)s")['time'] }}', 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_temperature_data_%(name)s")['id'] }}, 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_temperature_data_%(name)s")['min'] }}, 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_temperature_data_%(name)s")['max'] }}
)
"""


def get_temperature_data_open_meteo(ds: str, base_url: str, lon: float, lat: float, city_id: str) -> Dict[str, Any]:
    """
    Makes an HTTP GET action to retrieve the data of the city temperature
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


def save_temperature_data(temperature_data: Dict[str, Any]) -> None:
    """
    Prints the data received from the previous task
    :param temperature_data: dict with temperature data of the city
    """
    print(temperature_data)

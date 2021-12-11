"""
Functions used for manage open meteo data about humidity
"""
import requests

from typing import Dict, Any, List

INSERT_INTO_STMT: str = """
INSERT INTO humidity (date, id, relative_humidity) VALUES (
    '{{ ti.xcom_pull(task_ids="process_%(name)s.get_humidity_data_%(name)s")['time'] }}', 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_humidity_data_%(name)s")['id'] }}, 
    {{ ti.xcom_pull(task_ids="process_%(name)s.get_humidity_data_%(name)s")['relative_humidity'] }}::float
)
"""


def get_humidity_data_open_meteo(ds: str, base_url: str, lon: float, lat: float, city_id: str) -> Dict[str, Any]:
    """
    Makes an HTTP GET action to retrieve the data of the city hourly relative humidity
    :param ds: date of the temperature to retrieve
    :param base_url: open meteo url
    :param lon: longitude of the city
    :param lat: latitude of the city
    :param city_id: id of the city
    :return: dict with humidity data
    """
    params: Dict[str, Any] = {
        'latitude': lat,
        'longitude': lon,
        'hourly': 'relativehumidity_2m',
        'timezone': 'UTC',
        'past_days': 2
    }
    response: requests.Response = requests.get(base_url, params='&'.join("%s=%s" % (k, v) for k, v in params.items()))
    response: Dict[str, Any] = response.json()
    index: int = response['hourly']['time'].index(f'{ds}T00:00')

    day_values: List[int] = response['hourly']['relativehumidity_2m'][index:index + 23 + 1]
    daily_average: float = sum(day_values) / len(day_values)

    return {
        'time': ds,
        'id': city_id,
        'relative_humidity': "{:.2f}".format(daily_average)
    }

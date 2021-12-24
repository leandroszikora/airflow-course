from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook

from typing import Dict, Any, List


class OpenMeteoMetricsOperator(BaseOperator):
    ui_color = '#ffa500'  # type: str
    ui_fgcolor = '#008000'  # type: str

    def __init__(self, city_data: Dict[str, Any], frequency: str, metrics: List[str], metrics_mapper: Dict[str, Any],
                 http_conn_id: str, *args, **kwargs):
        self.city_data = city_data
        self.frequency = frequency
        self.metrics = metrics
        self.metrics_mapper = metrics_mapper
        self.http_conn_id = http_conn_id
        super().__init__(*args, **kwargs)

    @property
    def hook(self):
        return HttpHook(method='GET', http_conn_id=self.http_conn_id)

    def _aggregate_values(self, values: List[Any], index: int):
        day_values: List[int] = values[index:index + 23 + 1]
        return sum(day_values) / len(day_values)

    def execute(self, context: Any):
        params: Dict[str, Any] = {
            'latitude': self.city_data['city']['coord']['lat'],
            'longitude': self.city_data['city']['coord']['lon'],
            'timezone': 'UTC',
            'past_days': 2,
            self.frequency: ','.join(self.metrics)
        }
        response = self.hook.run(endpoint='forecast/', data='&'.join("%s=%s" % (k, v) for k, v in params.items()))
        response = response.json()
        ds = context['ds']
        index = response[self.frequency]['time'].index(f'{ds}T00:00') if self.frequency == 'hourly' \
            else response[self.frequency]['time'].index(ds)

        result = {
            'time': ds,
            'id': self.city_data['id']
        }
        for metric in self.metrics:
            result[metric] = self._aggregate_values(response[self.frequency][metric], index) \
                if self.frequency == 'hourly' \
                else response[self.frequency][metric][index]

        for old_name, new_name in self.metrics_mapper.items():
            result[new_name] = result.pop(old_name)

        return result

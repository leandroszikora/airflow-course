FROM apache/airflow:2.2.3

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends vim

RUN pip install --no-cache-dir lxml

USER airflow
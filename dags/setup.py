from setuptools import setup

setup(
    name='AirflowDags',
    version='0.0.1',
    description='Package with airflow dags',
    author='Leandro Szikora',
    author_email='leandroszikora@hotmail.com',
    packages=[],
    install_requires=[
        'apache-airflow',
        'requests'
    ]
)
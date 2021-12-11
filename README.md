# Curso Airflow

## Requerimientos
* Docker - una vez que lo instalemos deberemos asignarle 4Gb o más para poder trabajar en nuestro entorno.
* Python >= 3.8
* IDE de preferencia. (Visual Studio Code, Pycharm, etc)

## ¿Cómo iniciar nuestro entorno de trabajo?
Para poder levantar nuestro entorno de trabajo, vamos a utilizar la versión oficial
de un archivo compose de Airflow. Este puede ser descargado con el siguiente comando:
```shell
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.2/docker-compose.yaml'
```
El archivo contiene los servicios necesarios para poder trabajar con Airflow localmente.

Primero creamos los directorios y nuestra variable de entorno `AIRFLOW_UID`:
```shell
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
Luego, con Docker Compose iniciamos la base de datos:
```shell
docker-compose up airflow-init
```
Y por último, desplegamos los servicios del `docker-compose.yml`:
```shell
docker-compose up
```

### Borrar el ambiente de trabajo
Para recuperar los recursos utilizados por este entorno podemos arrojar el siguiente comando:
```shell
docker-compose down --volumes --remove-orphans
```
Esto va a ser que se borren todos los datos de la db de Airflow, por eso hay que usar este comando con precaución.
Si lo que se desea es pausar los servicios cuando terminamos una práctica, solo tenemos que arrojar este comando:
```shell
docker-compose down
```
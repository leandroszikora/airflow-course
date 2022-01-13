# Cómo levantar Airflow en un clúster de Kubernetes local
## Requerimientos
* Docker Engine >= 20.10.11
* Helm >= 3.7.1
* Kubectl >= 1.22.3
* minikube >= 1.24.0

## Despliegue inicial
1. Levantar minikube
```shell
minikube start
```

2. Configurar las variables que se van a usar en el despliegue
```shell
# The name of the Airflow instance
# Used as the name for the Helm installation
export AIRFLOW_INSTANCE_NAME="airflow-course"
# Kubernetes namespace for the Airflow instance
export AIRFLOW_NAMESPACE="airflow-course"
# Fernet key for the Airflow instance
export AIRFLOW_FERNET_KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | openssl base64)
```

3. Crear un namespace para la instancia
```shell
kubectl create namespace $AIRFLOW_NAMESPACE
```

4. Crear los secrets que se usan para configurar la instancia
```shell
kubectl create secret generic fernet-key \
    --namespace $AIRFLOW_NAMESPACE \
    --from-literal=fernet-key=$AIRFLOW_FERNET_KEY
```

5. Modificar el archivo Dockerfile a gusto y luego ejecutar los siguientes comando
```shell
eval $(minikube docker-env)
```
```shell
docker build . -f infra/Dockerfile --tag my-airflow:0.0.1
```

6. Completar todos los placeholders del archivo `values.yml`

7. Desplegar el chart de helm con las configuraciones realizadas en el archivo `values.yml` 
```shell
helm repo add apache-airflow https://airflow.apache.org

helm install $AIRFLOW_INSTANCE_NAME \
    --values values.yml \
    --namespace $AIRFLOW_NAMESPACE \
    apache-airflow/airflow \
    --version 1.2.0 --debug
```

8. Creamos un port-forwarding a nuestra red
```shell
kubectl port-forward svc/airflow-course-webserver 8080:8080 --namespace $AIRFLOW_NAMESPACE
```

Si se necesita actualizar el helm chart o algún componente del clúster se debe ejecutar este comando:
```shell
helm upgrade $AIRFLOW_INSTANCE_NAME \
    --values values.yml \
    --namespace $AIRFLOW_NAMESPACE \
    apache-airflow/airflow \
    --version 1.2.0 --debug
```

Si queremos borrar todo:
```shell
helm uninstall $AIRFLOW_INSTANCE_NAME --namespace $AIRFLOW_NAMESPACE

kubectl delete namespace $AIRFLOW_NAMESPACE
```
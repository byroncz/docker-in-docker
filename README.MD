1. Instalar Airflow

export AIRFLOW_HOME=$pwd
pip install "apache-airflow[celery]==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"
pip install apache-airflow-providers-docker==3.9.1
pip install connexion[swagger-ui]==2.14.2

airflow db init
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

En un tab distinto correr:
airflow webserver -p 8080

En otro tab correr:
airflow scheduler

2. airflow db init

Construir imagenes de Python y Postgres

docker build -t python-image -f ./img/python/Dockerfile.python .

docker build -t postgres-image -f ./img/postgres/Dockerfile.postgres .
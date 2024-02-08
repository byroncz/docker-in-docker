from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount
from airflow.models import Variable
import json


LOCAL_WORKING_DIRECTORY = '/workspaces/reto'
WORKING_DIRECTORY = '/usr/src/app'

DOCKER_OPERATOR_ARGS = {
    'image':'processor:latest',
    'api_version': 'auto',
    'auto_remove': True,
    'docker_url':"unix://var/run/docker.sock", 
    'network_mode':"bridge",
    'mounts':[
        Mount(
            source=f'{LOCAL_WORKING_DIRECTORY}/monokera/src/scripts', 
            target=WORKING_DIRECTORY,
            type='bind',
        ),
        Mount(
            source=f'{LOCAL_WORKING_DIRECTORY}/monokera/src/data', 
            target=f'{WORKING_DIRECTORY}/data',
            type='bind',
        )
    ]    
}

CONTAINER_ENV_VARS={
    **json.loads(Variable.get("db_params")),
}

LOADING_STEPS_SCRIPTS = [
    'agents.py',
    'claims.py',
    'insured.py',
    'payments.py',
    'premium.py'
]

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3600),
}

with DAG(
    'monokera',
    default_args=DEFAULT_DAG_ARGS,
    description='Reto Monokera',
    schedule_interval=None,
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    cleaning = DockerOperator(
        task_id='cleaning',
        command=f"python {WORKING_DIRECTORY}/cleaning/cleaning.py",
        **DOCKER_OPERATOR_ARGS,
    )

    loading_policy = DockerOperator(
        task_id='loading_policy',
        command=f"python {WORKING_DIRECTORY}/loading/policy.py",
        **DOCKER_OPERATOR_ARGS,
        environment=CONTAINER_ENV_VARS
    )

    with TaskGroup("loading") as loading:
            for script in LOADING_STEPS_SCRIPTS:
                current_task = DockerOperator(
                    task_id=f'{script[:-3]}', 
                    command=f"python {WORKING_DIRECTORY}/loading/{script}",
                    **DOCKER_OPERATOR_ARGS,
                    environment=CONTAINER_ENV_VARS
                )

    reporting = DockerOperator(
        task_id='reporting',
        command=f"python {WORKING_DIRECTORY}/report/report.py", 
        **DOCKER_OPERATOR_ARGS,
        environment=CONTAINER_ENV_VARS
    )

    end = DummyOperator(
        task_id='end'
    )

start >> cleaning >> loading_policy >> loading >> reporting >> end

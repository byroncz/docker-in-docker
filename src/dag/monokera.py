from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount


LOCAL_WORKING_DIRECTORY = '/workspaces/reto'
WORKING_DIRECTORY = '/usr/src/app'
loading_scripts = [
    'agents.py',
    'claims.py',
    'insured.py',
    'payments.py',
    'premium.py'
]

default_args = {
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
    default_args=default_args,
    description='Un DAG para cargar datos en paralelo usando DockerOperator',
    schedule_interval=None,
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    cleaning = DockerOperator(
        task_id='cleaning',
        image='processor:latest',
        api_version='auto',
        auto_remove=True,
        command=f"python {WORKING_DIRECTORY}/cleaning/cleaning.py",
        docker_url="unix://var/run/docker.sock", 
        network_mode="bridge",
        mounts=[
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
    )

    loading_policy = DockerOperator(
        task_id='loading_policy',
        image='processor:latest',
        api_version='auto',
        auto_remove=True,
        command=f"python {WORKING_DIRECTORY}/loading/policy.py",
        docker_url="unix://var/run/docker.sock", 
        network_mode="bridge",
        mounts=[
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
    )

    with TaskGroup("loading") as loading:
            for script in loading_scripts:
                current_task = DockerOperator(
                    task_id=f'{script[:-3]}', 
                    image='processor:latest',
                    api_version='auto',
                    auto_remove=True,
                    command=f"python {WORKING_DIRECTORY}/loading/{script}",
                    docker_url="unix://var/run/docker.sock",
                    network_mode="bridge",
                    mounts=[
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
                )

    end = DummyOperator(
        task_id='end'
    )

start >> cleaning >> loading_policy >> loading >> end

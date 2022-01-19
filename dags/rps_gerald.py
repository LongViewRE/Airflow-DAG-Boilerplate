import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

credentials = {
        "gerald_password": BaseHook().get_connection('gerald').get_password(),
        "gerald_username": "/dbs/gerald/colls/clients",
        "rps_key" : Variable.get("rps_api_key")
    }

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

@dag(schedule_interval="0 0 * * *", start_date=datetime(2021, 12, 16), catchup=False, tags=['gerald', 'rps'],
    max_active_runs=1, default_args=default_args)


def rps_syncing():
    """
    This DAG handles syncing data to Gerald.
    """
    # Add modules to this list once complete (and pull, process, push methods implemented)
    modules = ["PullFromRPS"]
    task_types = ["pull", "process", "push"]

    tasks = {}
    for module in modules:
        tasks[module] = {}
        for task_type in task_types:
            tasks[module][task_type] = \
                    DockerOperator(
                        image="lvdocker.azurecr.io/gerald-syncing:latest", 
                        task_id=f'{module}_{task_type}',
                        command=f'{module} {task_type}',
                        private_environment=credentials,
                        tty=True,
                        force_pull=True,
                        mounts=[Mount(source="/home/geraldadmin/airflow/tmpdata", 
                                    target="/tmpdata", type="bind")]
                    )
    tasks["PullFromRPS"]["pull"] >> tasks["PullFromRPS"]["process"] >> tasks["PullFromRPS"]["push"]                        
    

rps_syncing = rps_syncing()
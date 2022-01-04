import json
from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator

credentials = {
        "gerald_password": BaseHook().get_connection('gerald').get_password(),
        "gerald_username": "/dbs/gerald/colls/clients",
        "rps_key" : Variable.get("rps_api_key")
    }

@dag(schedule_interval=None, start_date=datetime(2021, 12, 16), catchup=False, tags=['gerald'])
def gerald_syncing():
    """
    This DAG handles syncing data to Gerald.
    """
    
    task1 = DockerOperator(
        image="lvdocker.azurecr.io/gerald-syncing:latest", 
        task_id='PullFromRPS',
        command='PullFromRPS',
        private_environment=credentials
    )
        
gerald_syncing = gerald_syncing()
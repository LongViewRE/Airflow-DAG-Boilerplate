
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

h = BaseHook().get_connection('Gerald_Syncing')
m = BaseHook().get_connection('MS_Graph')
credentials = {
        "gerald_password": BaseHook().get_connection('gerald').get_password(),
        "gerald_username": "/dbs/gerald/colls/clients",
        "AZURE_TENANT_ID": h.extra_dejson['extra__azure__tenantId'],
        "AZURE_CLIENT_SECRET": h.password,
        "AZURE_CLIENT_ID": h.login,
        "MSGraph_Client_ID": m.login,
        "MSGraph_Secret": m.password
    }

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

@dag(schedule_interval="0 0 * */1 *", start_date=datetime(2021, 1, 20), catchup=False, tags=['gerald', 'azure'],
    max_active_runs=1, default_args=default_args)


def azure_syncing():
    """
    This DAG handles syncing data to Gerald.
    """
    # Add modules to this list once complete (and pull, process, push methods implemented)
    modules = ["PullFromAzure"]
    task_types = ["pull", "process", "pushgerald", "pushgr"]

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
    tasks["PullFromAzure"]["pull"] >> tasks["PullFromAzure"]["process"] >> tasks["PullFromAzure"]["pushgerald"] >> tasks["PullFromAzure"]['pushgr']                        
    

gerald_syncing = azure_syncing()
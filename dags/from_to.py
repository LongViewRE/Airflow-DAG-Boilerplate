
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


####CHANGE
# Use credentials to set environment variables according to Airflow documentation.
credentials = {
    }

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

##### CHANGE
# Adjust scheduling interval to how often you want the dag to be run. Make sure to adjust the tags as well. 
@dag(schedule_interval="0 0 * */1 *", start_date=datetime(2021, 1, 20), catchup=False, tags=['tag1', 'tag2'],
    max_active_runs=1, default_args=default_args)


#### CHANGE
# Adjust the name of function to whatever you would like it to look in Airflow. 
def submodule():
    """
    This DAG handles syncing data to Gerald.
    """
    
    #### CHANGE
    # Adjust modules and task_types to correspond to the submodule and tasks defined in submodule/sync.py
    modules = ["submodule"]
    task_types = ["pull", "process"]

    tasks = {}
    for module in modules:
        tasks[module] = {}
        for task_type in task_types:
            tasks[module][task_type] = \
                    DockerOperator(
                        #### CHANGE
                        # Adjust image to the repository in LVDocker. 
                        image="", 
                        task_id=f'{module}_{task_type}',
                        command=f'{module} {task_type}',
                        private_environment=credentials,
                        tty=True,
                        force_pull=True,
                        ### CHANGE
                        # Adjust mount source to the correct directory
                        mounts=[Mount(source="", 
                                    target="/tmpdata", type="bind")]
                    )
    #### CHANGE
    # Adjust the tasks as they should be run. Use the following format tasks['submodule']['task1'] >> tasks['submodule']['task2']                    
    

#### CHANGE
# Change to what the function above is doing. 
submodule = submodule()

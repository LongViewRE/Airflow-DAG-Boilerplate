import json
from datetime import datetime

from airflow.decorators import dag, task

@dag(schedule_interval=None, start_date=datetime(2021, 12, 16), catchup=False, tags=['gerald'])
def gerald_syncing():
    """
    This DAG handles syncing data to Gerald.
    """
    
    @task.virtualenv(
        use_dill=False,
        system_site_packages=True,
        requirements=['thefuzz[speedup]', 'python-dotenv', 
                        'git+ssh://git@github.com/LongViewRE/LV_db_connection',
                        'git+ssh://git@github.com/LongViewRE/LV_external_services',
                        'git+ssh://git@github.com/LongViewRE/LV_general_functions'],
    )
    def pull_from_RPS():
        # extremely hacky way to add packages to the python path, should be changed in future
        import sys
        sys.path.insert(0, "/home/geraldadmin/airflow/dags")

        import logging
        logging.getLogger().setLevel(logging.INFO)

        from gerald_syncing.PullFromRPS.sync import sync_test
        
        from airflow.hooks.base import BaseHook
        from LV_db_connection import GremlinClient
        
        p = BaseHook().get_connection('gerald').get_password()
        g = GremlinClient("/dbs/gerald/colls/clients", p)

        res = g.submit("g.V('1')")
        logging.info(str(len(res)))


    pull_from_RPS()

gerald_syncing = gerald_syncing()